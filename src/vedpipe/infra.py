"""Infrastructure bootstrap: create the MinIO buckets with boto3.

The MinIO client image (``mc``) is no longer published on Docker Hub, and a frozen
server image should not be trusted to ship helper binaries either. Creating buckets
from our own image with boto3 keeps the bootstrap on code we control, and the wait
loop removes any dependency on a MinIO healthcheck.
"""

from __future__ import annotations

import argparse
import logging
import time

from vedpipe.config import Settings, setup_logging
from vedpipe.storage import make_s3_client

log = logging.getLogger(__name__)


def ensure_buckets(settings: Settings, timeout_s: float = 90.0) -> list[str]:
    """Wait for the S3 endpoint, then create missing buckets. Idempotent.

    Returns the names of the buckets that were created by this call.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    client = make_s3_client(settings)
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
            break
        except (BotoCoreError, ClientError) as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"S3 endpoint {settings.s3_endpoint_url} not ready") from exc
            log.info("waiting for S3 endpoint %s (%s)", settings.s3_endpoint_url, exc)
            time.sleep(2)

    created = []
    for bucket in (settings.raw_bucket, settings.curated_bucket, settings.reports_bucket):
        if bucket in existing:
            continue
        try:
            client.create_bucket(Bucket=bucket)
            created.append(bucket)
        except ClientError as exc:
            # Another init container may have won the race; that is fine.
            if exc.response.get("Error", {}).get("Code") not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise
    log.info("buckets ready (created: %s)", ", ".join(created) or "none")
    return created


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["init-buckets"])
    parser.parse_args(argv)
    setup_logging()
    ensure_buckets(Settings.from_env())


if __name__ == "__main__":
    main()
