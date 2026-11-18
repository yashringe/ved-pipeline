"""Runtime configuration, read once from environment variables.

Every container gets the same variables from docker-compose, so one immutable
``Settings`` object is the single place where names and local defaults live. The
defaults only work against the local compose stack; nothing here is a real secret.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

# Partner ids become part of object keys (partner=<id>/), so keep them path-safe.
_PARTNER_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def parse_api_keys(spec: str) -> dict[str, str]:
    """Parse ``"partner:key,partner2:key2"`` into ``{key: partner}``.

    The map is keyed by API key because authentication looks a partner up by the key
    it presents; the partner id is never taken from the request body.
    """
    keys: dict[str, str] = {}
    for raw_item in spec.split(","):
        item = raw_item.strip()
        if not item:
            continue
        partner, sep, key = (part.strip() for part in item.partition(":"))
        if not sep or not key or not _PARTNER_ID.match(partner):
            raise ValueError(f"API_KEYS entry {item!r} is not 'partner:key' with a safe partner id")
        if key in keys:
            raise ValueError(f"API key for partner {partner!r} is not unique")
        keys[key] = partner
    return keys


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    """All tunables of the pipeline. Build with :meth:`from_env`."""

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"  # local demo default only
    s3_region: str = "us-east-1"
    raw_bucket: str = "raw"
    curated_bucket: str = "curated"
    reports_bucket: str = "reports"
    pg_dsn: str = "postgresql://ved:ved@localhost:5432/ved"
    api_keys: dict[str, str] = field(default_factory=dict)
    api_url: str = "http://localhost:8000"
    api_key: str = "dev-key-a"
    ved_data_dir: Path = Path("data/ved")
    ved_weeks: int = 2
    sql_dir: Path = Path("sql")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            s3_endpoint_url=_env("S3_ENDPOINT_URL", cls.s3_endpoint_url),
            s3_access_key=_env("S3_ACCESS_KEY", cls.s3_access_key),
            s3_secret_key=_env("S3_SECRET_KEY", cls.s3_secret_key),
            s3_region=_env("S3_REGION", cls.s3_region),
            raw_bucket=_env("RAW_BUCKET", cls.raw_bucket),
            curated_bucket=_env("CURATED_BUCKET", cls.curated_bucket),
            reports_bucket=_env("REPORTS_BUCKET", cls.reports_bucket),
            pg_dsn=_env("PG_DSN", cls.pg_dsn),
            api_keys=parse_api_keys(_env("API_KEYS", "partner-a:dev-key-a")),
            api_url=_env("API_URL", cls.api_url),
            api_key=_env("API_KEY", cls.api_key),
            ved_data_dir=Path(_env("VED_DATA_DIR", str(cls.ved_data_dir))),
            ved_weeks=int(_env("VED_WEEKS", str(cls.ved_weeks))),
            sql_dir=Path(_env("SQL_DIR", str(cls.sql_dir))),
        )


def setup_logging() -> None:
    """Uniform, grep-friendly log lines for every CLI entry point."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
