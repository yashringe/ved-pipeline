"""Simulator: VED header mapping, DayNum handling, payload shape and the fault plan."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import requests

from vedpipe.hashing import content_sha256
from vedpipe.schema import SIGNAL_FIELDS
from vedpipe.sim.replay import FaultRates, ReplayError, Sender, plan_deliveries
from vedpipe.sim.ved import (
    MS_PER_DAY,
    build_payload,
    iter_trip_payloads,
    map_columns,
    map_header,
    trip_start,
    week_date,
)

VED_HEADERS = [
    "DayNum", "VehId", "Trip", "Timestamp(ms)", "Latitude[deg]", "Longitude[deg]",
    "Vehicle Speed[km/h]", "MAF[g/sec]", "Engine RPM[RPM]", "Absolute Load[%]",
    "OAT[DegC]", "Fuel Rate[L/hr]", "Air Conditioning Power[kW]",
    "Air Conditioning Power[Watts]", "Heater Power[Watts]", "HV Battery Current[A]",
    "HV Battery SOC[%]", "HV Battery Voltage[V]", "Short Term Fuel Trim Bank 1[%]",
    "Short Term Fuel Trim Bank 2[%]", "Long Term Fuel Trim Bank 1[%]",
    "Long Term Fuel Trim Bank 2[%]",
]
NOON_EDT = "2017-11-01T12:00:00.000-04:00"


def test_header_mapping_covers_every_ved_column() -> None:
    mapping = map_columns(VED_HEADERS)
    assert list(mapping.values()) == ["day_num", "vehicle_id", "trip_id", "t_ms", *SIGNAL_FIELDS]


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Outside Air Temperature[DegC]", "oat_c"),  # README spelling
        ("  vehicle speed (km/h) ", "speed_kmh"),
        ("VEHICLE_SPEED", "speed_kmh"),  # unit-less fallback
        ("hv battery current", "hv_current_a"),
        ("Air Conditioning Power", None),  # kW or W? ambiguous without its unit
        ("Some New Signal[x]", None),
    ],
)
def test_header_variants(header: str, expected: str | None) -> None:
    assert map_header(header) == expected


def test_missing_key_column_is_an_error() -> None:
    with pytest.raises(ValueError, match="t_ms"):
        map_columns(["DayNum", "VehId", "Trip", "Vehicle Speed[km/h]"])


def test_daynum_constant_per_trip() -> None:
    start = trip_start(np.array([1.5, 1.5, 1.5]), np.array([0, 1000, 2000]))
    assert start.isoformat(timespec="milliseconds") == NOON_EDT


def test_daynum_advancing_per_row() -> None:
    t = np.array([5000.0, 6000.0, 7000.0])
    start = trip_start(1.5 + t / MS_PER_DAY, t)
    assert start.isoformat(timespec="milliseconds") == NOON_EDT


def test_daynum_after_dst_change_uses_standard_time() -> None:
    # DayNum 7.5 = 2017-11-07 12:00 local, after the 5 Nov switch to EST (-05:00).
    start = trip_start(np.array([7.5]), np.array([0]))
    assert start.isoformat() == "2017-11-07T12:00:00-05:00"


def rows(**columns: Any) -> pd.DataFrame:
    return pd.DataFrame(columns)


def test_nan_signals_are_omitted_and_same_t_rows_merged() -> None:
    frame = rows(
        day_num=[1.5] * 4,
        t_ms=[0, 0, 1000, 2000],
        speed_kmh=[10.0, np.nan, 12.0, np.nan],
        lat=[np.nan, 42.28, np.nan, np.nan],
        lon=[np.nan, -83.74, np.nan, np.nan],
        maf_g_s=[np.nan, np.nan, np.nan, np.nan],
    )
    payload = build_payload(8, 706, frame)

    assert payload is not None
    assert payload["started_at"] == NOON_EDT
    # t=0 rows merge; t=2000 has no signal at all and is dropped.
    assert payload["points"] == [
        {"t": 0, "speed_kmh": 10.0, "lat": 42.28, "lon": -83.74},
        {"t": 1000, "speed_kmh": 12.0},
    ]


def test_csv_to_payloads_end_to_end(tmp_path: Path) -> None:
    path = tmp_path / "VED_171101_week.csv"
    frame = pd.DataFrame(np.nan, index=range(4), columns=VED_HEADERS)
    frame["DayNum"] = [1.6, 1.6, 1.5, 1.5]
    frame["VehId"] = [9, 9, 8, 8]
    frame["Trip"] = [1, 1, 2, 2]
    frame["Timestamp(ms)"] = [0, 1000, 0, 1000]
    frame["Vehicle Speed[km/h]"] = [5.0, 6.0, 7.0, 8.0]
    frame.to_csv(path, index=False)

    payloads = list(iter_trip_payloads([path]))

    assert [(p["vehicle_id"], p["trip_id"]) for p in payloads] == [(8, 2), (9, 1)]  # by start
    assert payloads[0]["points"] == [{"t": 0, "speed_kmh": 7.0}, {"t": 1000, "speed_kmh": 8.0}]


@pytest.mark.parametrize(
    "name", ["VED_171101_week.csv", "VED_110117_week.csv", "x/VED_171101_week.csv"]
)
def test_week_date_accepts_both_documented_and_observed_names(name: str) -> None:
    assert week_date(name).isoformat() == "2017-11-01"


def test_week_date_rejects_other_files() -> None:
    assert week_date("VED_999999_week.csv") is None
    assert week_date("notes.csv") is None


def synthetic_trips(n: int = 400) -> list[dict[str, Any]]:
    base = datetime.fromisoformat("2017-11-01T06:00:00-04:00")
    return [
        {
            "vehicle_id": 1 + i % 7,
            "trip_id": i + 1,
            "started_at": (base + timedelta(minutes=20 * i)).isoformat(timespec="milliseconds"),
            "points": [{"t": s * 1000, "speed_kmh": 30.0} for s in range(0, 600, 10)],
        }
        for i in range(n)
    ]


HIGH_RATES = FaultRates(duplicate=0.2, late=0.2, partial=0.2, speed_spike=0.2)


def test_plan_is_deterministic_for_a_seed() -> None:
    def run(seed: int) -> list[tuple[str, str, str]]:
        return [
            (d.sent_at.isoformat(), d.kind, json.dumps(d.payload, sort_keys=True))
            for d in plan_deliveries(synthetic_trips(), HIGH_RATES, seed)
        ]

    assert run(7) == run(7)
    assert run(7) != run(8)


def test_plan_is_sorted_and_contains_every_fault_kind() -> None:
    truth: list[dict[str, Any]] = []
    plan = list(plan_deliveries(synthetic_trips(), HIGH_RATES, seed=1, ground_truth=truth))

    assert [d.sent_at for d in plan] == sorted(d.sent_at for d in plan)
    assert {kind for t in truth for kind in t["faults"]} == {
        "duplicate", "late", "partial", "speed_spike",
    }
    assert Counter(d.kind for d in plan).keys() == {"normal", "late", "partial", "duplicate"}
    trips_delivered = {(d.payload["vehicle_id"], d.payload["trip_id"]) for d in plan}
    assert len(trips_delivered) == 400


def test_fault_semantics() -> None:
    truth: list[dict[str, Any]] = []
    plan = list(plan_deliveries(synthetic_trips(), HIGH_RATES, seed=3, ground_truth=truth))
    by_trip: dict[int, list[Any]] = {}
    for delivery in plan:
        by_trip.setdefault(delivery.payload["trip_id"], []).append(delivery)

    def trip_end(payload: dict[str, Any]) -> datetime:
        last = max(p["t"] for p in payload["points"])
        return datetime.fromisoformat(payload["started_at"]) + timedelta(milliseconds=last)

    for deliveries in by_trip.values():
        full = [d for d in deliveries if d.kind in ("normal", "late")]
        assert len(full) == 1
        end = trip_end(full[0].payload)
        for d in deliveries:
            if d.kind == "late":
                assert d.sent_at - end > timedelta(hours=24)
            if d.kind == "normal":
                assert d.sent_at - end < timedelta(hours=1)
            if d.kind == "duplicate":
                assert content_sha256(d.payload) == content_sha256(full[0].payload)
                assert timedelta(hours=1) <= d.sent_at - full[0].sent_at <= timedelta(hours=6)
            if d.kind == "partial":
                assert len(d.payload["points"]) < len(full[0].payload["points"])
                assert d.sent_at < end
    spiked = [t for t in truth if "speed_spike" in t["faults"]]
    assert spiked
    for t in spiked:
        [full] = [d for d in by_trip[t["trip_id"]] if d.kind in ("normal", "late")]
        assert sum(p["speed_kmh"] == 320.0 for p in full.payload["points"]) == 1


class FakeResponse:
    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self.headers = headers or {}
        self.text = "body"


class FakeSession:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    def post(self, *args: Any, **kwargs: Any) -> FakeResponse:
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_sender_retries_transient_errors_then_succeeds() -> None:
    sleeps: list[float] = []
    session = FakeSession([
        requests.ConnectionError("down"), FakeResponse(503, {"Retry-After": "2"}),
        FakeResponse(429), FakeResponse(202),
    ])
    Sender("http://api", "k", session=session, sleep=sleeps.append).send([{}])  # type: ignore[arg-type]
    assert session.calls == 4
    assert sleeps == [0.5, 2.0, 2.0]


def test_sender_does_not_retry_a_rejected_batch() -> None:
    session = FakeSession([FakeResponse(422)])
    with pytest.raises(ReplayError, match="422"):
        Sender("http://api", "k", session=session, sleep=lambda _: None).send([{}])  # type: ignore[arg-type]
    assert session.calls == 1


def test_fetch_selects_first_weeks_and_detects_lfs_pointers(tmp_path: Path) -> None:
    fetch = pytest.importorskip("vedpipe.sim.fetch", reason="py7zr not installed")
    names = ["d/VED_171108_week.csv", "d/VED_180102_week.csv", "d/VED_171101_week.csv", "x.txt"]
    # Chronological, not lexical: 180102 (2 Jan 2018) sorts after both November weeks.
    assert fetch.select_weeks(names, 2) == ["d/VED_171101_week.csv", "d/VED_171108_week.csv"]
    assert len(fetch.select_weeks(names, 0)) == 3

    pointer = tmp_path / "VED_DynamicData_Part1.7z"
    pointer.write_text("version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1\n")
    with pytest.raises(fetch.FetchError, match="LFS"):
        fetch.ensure_not_lfs_pointer(pointer)
