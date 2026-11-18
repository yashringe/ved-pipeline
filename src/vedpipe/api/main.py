"""FastAPI ingest service.

Each request is persisted as exactly one NDJSON object in the raw bucket, one line per
trip, wrapped in an envelope (record_id, received_at, partner_id, content_sha256,
payload). The service does no deduplication or business validation: it is a durable,
append-only landing zone, and everything else is recomputed downstream from it.

Run with ``uvicorn vedpipe.api.main:create_app --factory``.
"""

# No ``from __future__ import annotations`` here: FastAPI resolves endpoint annotations at
# runtime, and the auth dependency is a closure that module globals cannot see.
import hmac
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, status

from vedpipe.api.models import (
    CONTRACT_VERSION,
    AcceptedRecord,
    IngestResponse,
    TripBatch,
    TripPayload,
)
from vedpipe.config import Settings
from vedpipe.hashing import canonical_json, content_sha256
from vedpipe.storage import ObjectStorage, S3Storage, StorageError, make_s3_client

log = logging.getLogger(__name__)

RAW_PREFIX = "trips"


def object_key(received_at: datetime, partner_id: str) -> str:
    """Hive-style key: lets Spark derive ingest_date and lets ops find a partner's data."""
    return (
        f"{RAW_PREFIX}/ingest_date={received_at:%Y-%m-%d}/partner={partner_id}/"
        f"{received_at:%H%M%S%f}-{uuid4().hex}.jsonl"
    )


def create_app(
    storage: ObjectStorage | None = None,
    api_keys: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Application factory; dependencies are injectable for tests, else read from env."""
    if storage is None or api_keys is None:
        settings = Settings.from_env()
        if storage is None:
            storage = S3Storage(make_s3_client(settings), settings.raw_bucket)
        if api_keys is None:
            api_keys = settings.api_keys
    keys = dict(api_keys)
    now = clock or (lambda: datetime.now(UTC))
    store: ObjectStorage = storage

    app = FastAPI(title="VED partner ingest API", version=CONTRACT_VERSION)

    def authenticate(x_api_key: Annotated[str | None, Header()] = None) -> str:
        """Resolve the partner from the key; constant-time compare against every key."""
        presented = (x_api_key or "").encode()
        partner = None
        for key, candidate in keys.items():
            if hmac.compare_digest(key.encode(), presented):
                partner = candidate
        if not presented or partner is None:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail="missing or invalid X-API-Key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        return partner

    Partner = Annotated[str, Depends(authenticate)]

    def persist(trips: list[TripPayload], partner_id: str) -> IngestResponse:
        received_at = now()
        envelopes: list[dict[str, Any]] = []
        for trip in trips:
            payload = trip.model_dump(mode="json", exclude_none=True)
            envelopes.append(
                {
                    "record_id": uuid4().hex,
                    "received_at": received_at.isoformat(),
                    "partner_id": partner_id,
                    "content_sha256": content_sha256(payload),
                    "payload": payload,
                }
            )
        key = object_key(received_at, partner_id)
        body = "".join(canonical_json(env) + "\n" for env in envelopes).encode("utf-8")
        try:
            store.put(key, body, "application/x-ndjson")
        except StorageError:
            log.exception("raw write failed for partner %s", partner_id)
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="storage unavailable, retry later",
                headers={"Retry-After": "5"},
            ) from None
        log.info("accepted %d trip(s) from %s -> %s", len(envelopes), partner_id, key)
        return IngestResponse(
            object_key=key,
            received_at=received_at,
            records=[
                AcceptedRecord(
                    record_id=env["record_id"],
                    vehicle_id=env["payload"]["vehicle_id"],
                    trip_id=env["payload"]["trip_id"],
                    content_sha256=env["content_sha256"],
                )
                for env in envelopes
            ],
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/trips", status_code=status.HTTP_202_ACCEPTED)
    def ingest_trip(trip: TripPayload, partner_id: Partner) -> IngestResponse:
        return persist([trip], partner_id)

    @app.post("/v1/trips/batch", status_code=status.HTTP_202_ACCEPTED)
    def ingest_batch(batch: TripBatch, partner_id: Partner) -> IngestResponse:
        return persist(batch.trips, partner_id)

    return app
