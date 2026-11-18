"""Object storage behind a small Protocol.

The API and the DQ report only ever *write whole objects*, so the interface is one
method. Production code uses :class:`S3Storage` (MinIO locally, any S3 API in the cloud);
tests use :class:`InMemoryStorage` and can make it fail on demand to exercise the
503 path without a network.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from vedpipe.config import Settings


class StorageError(RuntimeError):
    """Raised when an object could not be persisted. Callers treat it as retryable."""


class ObjectStorage(Protocol):
    """A bucket-scoped object store."""

    def put(self, key: str, body: bytes, content_type: str) -> None:
        """Persist ``body`` under ``key`` atomically, or raise :class:`StorageError`."""
        ...


def make_s3_client(settings: Settings) -> Any:
    """boto3 S3 client configured for MinIO (path-style addressing, explicit region)."""
    import boto3  # optional dependency: only S3-backed stages install it
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


class S3Storage:
    """Writes objects to one S3 bucket; any client or service error becomes StorageError."""

    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def put(self, key: str, body: bytes, content_type: str) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self._client.put_object(
                Bucket=self._bucket, Key=key, Body=body, ContentType=content_type
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"put s3://{self._bucket}/{key} failed: {exc}") from exc


class InMemoryStorage:
    """Thread-safe dict-backed store for tests; ``fail=True`` simulates an outage."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.fail = False
        self._lock = threading.Lock()

    def put(self, key: str, body: bytes, content_type: str) -> None:
        if self.fail:
            raise StorageError("simulated storage outage")
        with self._lock:
            self.objects[key] = (body, content_type)
