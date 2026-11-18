"""Ingest API: envelope layout, auth, structural validation, batching and 503 semantics."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from vedpipe.api.models import TripBatch, TripPayload
from vedpipe.hashing import content_sha256
from vedpipe.storage import InMemoryStorage

HttpClient = Any  # fastapi.testclient.TestClient, imported lazily by the fixture

NOW = datetime(2026, 11, 17, 18, 30, 5, 123456, tzinfo=UTC)
KEY = "key-p1"


def trip(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "vehicle_id": 8,
        "trip_id": 706,
        "started_at": "2017-11-01T12:00:00-04:00",
        "sent_at": "2017-11-01T12:30:00-04:00",
        "points": [
            {"t": 0, "speed_kmh": 0.0, "lat": 42.28, "lon": -83.74},
            {"t": 1000, "speed_kmh": 3.5, "maf_g_s": 2.1},
        ],
    }
    body.update(overrides)
    return body


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


@pytest.fixture
def client(storage: InMemoryStorage) -> HttpClient:
    """HTTP-level tests need FastAPI; model-level tests below run without it."""
    pytest.importorskip("fastapi", reason="fastapi not installed (pip install -e .[api,dev])")
    from fastapi.testclient import TestClient

    from vedpipe.api.main import create_app

    return TestClient(create_app(storage=storage, api_keys={KEY: "p1"}, clock=lambda: NOW))


def post(client: HttpClient, path: str, body: Any, key: str | None = KEY) -> Any:
    return client.post(path, json=body, headers={"X-API-Key": key} if key else {})


def stored_envelopes(storage: InMemoryStorage) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for body, _ in storage.objects.values()
        for line in body.decode().splitlines()
    ]


def test_envelope_and_key_layout(client: HttpClient, storage: InMemoryStorage) -> None:
    response = post(client, "/v1/trips", trip(firmware="2.1"))

    assert response.status_code == 202
    [(key, (body, content_type))] = storage.objects.items()
    assert re.fullmatch(
        r"trips/ingest_date=2026-11-17/partner=p1/183005123456-[0-9a-f]{32}\.jsonl", key
    )
    assert content_type == "application/x-ndjson"
    [envelope] = [json.loads(line) for line in body.decode().splitlines()]
    assert set(envelope) == {"record_id", "received_at", "partner_id", "content_sha256", "payload"}
    assert envelope["partner_id"] == "p1"
    assert envelope["received_at"] == NOW.isoformat()
    assert envelope["payload"]["firmware"] == "2.1"  # unknown fields are kept
    assert envelope["content_sha256"] == content_sha256(envelope["payload"])
    assert response.json()["records"] == [
        {
            "record_id": envelope["record_id"],
            "vehicle_id": 8,
            "trip_id": 706,
            "content_sha256": envelope["content_sha256"],
        }
    ]
    assert response.json()["object_key"] == key


def test_partner_comes_from_key_not_payload(client: HttpClient, storage: InMemoryStorage) -> None:
    post(client, "/v1/trips", trip(partner_id="someone-else"))
    [envelope] = stored_envelopes(storage)
    assert envelope["partner_id"] == "p1"


def test_missing_signals_are_not_zero_filled(client: HttpClient, storage: InMemoryStorage) -> None:
    post(client, "/v1/trips", trip())
    [envelope] = stored_envelopes(storage)
    assert envelope["payload"]["points"][0] == {"t": 0, "speed_kmh": 0.0, "lat": 42.28,
                                                "lon": -83.74}


def test_resend_with_new_sent_at_keeps_the_hash(client: HttpClient) -> None:
    first = post(client, "/v1/trips", trip(sent_at="2017-11-01T12:30:00-04:00"))
    resend = post(client, "/v1/trips", trip(sent_at="2017-11-01T15:00:00-04:00"))
    changed = post(client, "/v1/trips", trip(points=[{"t": 0, "speed_kmh": 1.0}]))

    hashes = [r.json()["records"][0]["content_sha256"] for r in (first, resend, changed)]
    assert hashes[0] == hashes[1]
    assert hashes[2] != hashes[0]


@pytest.mark.parametrize("key", [None, "wrong-key"])
def test_missing_or_wrong_key_is_401(client: HttpClient, storage: InMemoryStorage,
                                     key: str | None) -> None:
    response = post(client, "/v1/trips", trip(), key=key)
    assert response.status_code == 401
    assert not storage.objects


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(trip(points=[]), id="empty-points"),
        pytest.param(trip(started_at="2017-11-01T12:00:00"), id="naive-datetime"),
        pytest.param(trip(sent_at="2017-11-01T12:30:00"), id="naive-sent-at"),
        pytest.param(trip(trip_id="abc"), id="wrong-type-trip-id"),
        pytest.param(trip(points=[{"t": 0, "speed_kmh": "fast"}]), id="wrong-type-signal"),
        pytest.param(trip(points=[{"t": -1, "speed_kmh": 1.0}]), id="negative-t"),
        pytest.param(trip(trip_id=0), id="trip-id-not-positive"),
        pytest.param({k: v for k, v in trip().items() if k != "vehicle_id"}, id="no-vehicle"),
    ],
)
def test_structural_errors_are_422(client: HttpClient, storage: InMemoryStorage,
                                   body: dict[str, Any]) -> None:
    assert post(client, "/v1/trips", body).status_code == 422
    assert not storage.objects


@pytest.mark.parametrize(
    "point",
    ['{"t": 0, "speed_kmh": NaN}', '{"t": 0, "speed_kmh": Infinity}', '{"t": 0, "x": NaN}'],
)
def test_non_finite_numbers_are_422(client: HttpClient, storage: InMemoryStorage,
                                    point: str) -> None:
    raw = (
        '{"vehicle_id": 1, "trip_id": 1, "started_at": "2017-11-01T12:00:00Z", '
        f'"points": [{point}]}}'
    )
    response = client.post(
        "/v1/trips", content=raw,
        headers={"X-API-Key": KEY, "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert not storage.objects


def test_implausible_values_are_accepted(client: HttpClient) -> None:
    body = trip(points=[{"t": 0, "speed_kmh": 999.0, "lat": 0.0, "lon": 0.0,
                         "engine_rpm": -5.0, "hv_soc_pct": 250.0}])
    assert post(client, "/v1/trips", body).status_code == 202


def test_batch_is_one_object(client: HttpClient, storage: InMemoryStorage) -> None:
    trips = [trip(trip_id=i) for i in (1, 2, 3)]
    response = post(client, "/v1/trips/batch", {"trips": trips})

    assert response.status_code == 202
    assert len(storage.objects) == 1
    envelopes = stored_envelopes(storage)
    assert [e["payload"]["trip_id"] for e in envelopes] == [1, 2, 3]
    assert len({e["record_id"] for e in envelopes}) == 3
    assert [r["trip_id"] for r in response.json()["records"]] == [1, 2, 3]


def test_batch_limit_is_500(client: HttpClient) -> None:
    assert post(client, "/v1/trips/batch", {"trips": [trip()] * 501}).status_code == 422
    assert post(client, "/v1/trips/batch", {"trips": []}).status_code == 422


def test_storage_failure_is_retryable_503(client: HttpClient, storage: InMemoryStorage) -> None:
    storage.fail = True
    response = post(client, "/v1/trips", trip())
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"


def test_healthz_needs_no_key(client: HttpClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


# --- contract model (no HTTP stack needed) ----------------------------------------------
def test_model_keeps_extras_and_drops_absent_signals() -> None:
    payload = TripPayload.model_validate(trip(firmware={"v": 2})).model_dump(
        mode="json", exclude_none=True
    )
    assert payload["firmware"] == {"v": 2}
    assert payload["points"][1] == {"t": 1000, "speed_kmh": 3.5, "maf_g_s": 2.1}
    assert payload["started_at"] == "2017-11-01T12:00:00-04:00"


@pytest.mark.parametrize(
    "body",
    [
        trip(started_at="2017-11-01T12:00:00"),
        trip(points=[{"t": 0, "speed_kmh": float("nan")}]),
        trip(points=[{"t": 0, "lat": float("inf")}]),
        trip(extra_metrics=[1.0, float("nan")]),
        trip(points=[{"t": 0, "nested": {"x": float("-inf")}}]),
    ],
    ids=["naive", "nan-signal", "inf-signal", "nan-extra", "inf-nested-extra"],
)
def test_model_rejects_naive_datetimes_and_non_finite_numbers(body: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        TripPayload.model_validate(body)


def test_model_batch_bounds() -> None:
    assert len(TripBatch.model_validate({"trips": [trip()] * 500}).trips) == 500
    with pytest.raises(ValidationError):
        TripBatch.model_validate({"trips": [trip()] * 501})
