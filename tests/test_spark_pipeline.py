"""Curation logic on a hand-built raw folder.

The same synthetic trips are checked twice:
* against the pandas kernel directly (no JVM needed), and
* end to end through the Spark job (read raw NDJSON -> curated Parquet), which also
  covers corrupt lines, exact resends, superseded partial versions, duplicate samples,
  lateness and ingest statistics.

Base trip: 101 samples at 1 Hz (t = 0..100 s), 36 km/h (10 m/s), MAF 10 g/s, a GPS fix
every 3 s moving north at 10 m/s.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from vedpipe.hashing import content_sha256
from vedpipe.spark import kernel

BASE_LAT, BASE_LON = 42.28, -83.74
M_PER_DEG_LAT = 111_195.0
START = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)
END = START + timedelta(seconds=100)
RECEIVED = datetime(2026, 11, 17, 9, 0, tzinfo=UTC)
ON_TIME = END + timedelta(minutes=5)
FUEL_L = 10 / 14.08 / 745 * 100  # MAF 10 g/s for 100 s, AFR 14.08, 745 g/L
EV_KWH = 350 * 20 * 100 / 3.6e6  # 350 V x 20 A discharge for 100 s


def make_points(
    *,
    speed: Callable[[int], float] = lambda s: 36.0,
    ev: bool = False,
    gps_drop: range = range(0),
    gps_shift_at: int | None = None,
    dup_at: int | None = None,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for s in range(101):
        point: dict[str, Any] = {"t": s * 1000, "speed_kmh": speed(s)}
        if ev:
            point.update(hv_voltage_v=350.0, hv_current_a=-20.0)
        else:
            point["maf_g_s"] = 10.0
        if s % 3 == 0 and s not in gps_drop:
            shift = 0.05 if s == gps_shift_at else 0.0
            point.update(lat=BASE_LAT + s * 10 / M_PER_DEG_LAT + shift, lon=BASE_LON)
        points.append(point)
        if s == dup_at:
            points.append(dict(point))
    return points


CASES: dict[str, dict[str, Any]] = {
    "clean": {},
    "spike": {"speed": lambda s: 320.0 if s == 50 else 36.0},
    "gps_gap": {"gps_drop": range(31, 99)},
    "gps_jump": {"gps_shift_at": 51},
    "idle": {"speed": lambda s: 0.0 if s < 20 else 36.0},
    "ev": {"ev": True},
}


def kernel_metrics(**kwargs: Any) -> dict[str, Any]:
    frame = pd.DataFrame(make_points(**kwargs)).assign(vehicle_id=1, trip_id=1)
    frame = frame.rename(columns={"t": "t_ms"})
    return kernel.trip_metrics(kernel.clean_points(frame)).iloc[0].to_dict()


# --- kernel (no Spark) -----------------------------------------------------------------
def test_kernel_clean_trip() -> None:
    m = kernel_metrics(**CASES["clean"])
    assert m["duration_s"] == 100
    assert m["distance_km"] == pytest.approx(1.0)
    assert m["moving_s"] == 100
    assert m["idle_s"] == 0
    assert m["fuel_l"] == pytest.approx(FUEL_L)
    assert m["fuel_coverage"] == pytest.approx(1.0)
    assert m["elec_kwh"] == 0
    assert m["n_gps_fixes"] == 34
    assert m["max_gps_gap_s"] == 3
    assert m["n_gps_jumps"] == m["n_accel_spikes"] == m["n_speed_out_of_range"] == 0


def test_kernel_speed_spike_is_nulled_and_held_over() -> None:
    m = kernel_metrics(**CASES["spike"])
    assert m["n_speed_out_of_range"] == 1
    assert m["n_accel_spikes"] == 0
    assert m["distance_km"] == pytest.approx(1.0)
    assert m["max_speed_kmh"] == 36


def test_kernel_accel_spike_between_valid_samples() -> None:
    m = kernel_metrics(speed=lambda s: 150.0 if s == 50 else 36.0)  # +114 km/h in 1 s
    assert m["n_speed_out_of_range"] == 0
    assert m["n_accel_spikes"] == 2  # up and back down


def test_kernel_interior_gps_gap() -> None:
    m = kernel_metrics(**CASES["gps_gap"])
    assert m["max_gps_gap_s"] == 69


def test_kernel_gps_jump() -> None:
    m = kernel_metrics(**CASES["gps_jump"])
    assert m["n_gps_jumps"] == 2  # into the bad fix and back out


def test_kernel_idle_and_moving_time() -> None:
    m = kernel_metrics(**CASES["idle"])
    assert m["idle_s"] == 20
    assert m["moving_s"] == 80
    assert m["distance_km"] == pytest.approx(0.8)


def test_kernel_ev_battery_energy() -> None:
    m = kernel_metrics(**CASES["ev"])
    assert m["elec_kwh"] == pytest.approx(EV_KWH)
    assert m["elec_kwh"] == pytest.approx(0.1944, abs=1e-4)
    assert m["batt_coverage"] == pytest.approx(1.0)
    assert m["fuel_l"] == 0
    assert m["fuel_coverage"] == 0


def test_kernel_hold_is_bounded() -> None:
    # Speed only every 5 s: held for 3 s, so 4 of every 5 one-second steps are covered.
    frame = pd.DataFrame({"t_ms": [s * 1000 for s in range(101)], "vehicle_id": 1, "trip_id": 1,
                          "speed_kmh": [36.0 if s % 5 == 0 else None for s in range(101)]})
    m = kernel.trip_metrics(kernel.clean_points(frame)).iloc[0]
    assert m["distance_km"] == pytest.approx(0.8)
    assert m["moving_s"] == 80


def test_kernel_steps_over_10_s_are_gaps() -> None:
    seconds = [*range(0, 51), *range(70, 101)]
    frame = pd.DataFrame({"t_ms": [s * 1000 for s in seconds], "vehicle_id": 1, "trip_id": 1,
                          "speed_kmh": 36.0})
    m = kernel.trip_metrics(kernel.clean_points(frame)).iloc[0]
    assert m["gap_s"] == 20
    assert m["distance_km"] == pytest.approx(0.8)
    assert m["duration_s"] == 100


def test_kernel_fuel_rate_wins_over_maf_and_trims_are_averaged() -> None:
    base = {"t_ms": [0, 1000], "vehicle_id": 1, "trip_id": 1, "maf_g_s": 10.0}
    direct = pd.DataFrame({**base, "fuel_rate_l_h": 3.6})
    assert kernel.trip_metrics(kernel.clean_points(direct)).iloc[0]["fuel_l"] == pytest.approx(
        0.001  # 3.6 L/h for 1 s
    )
    trims = pd.DataFrame({**base, "stft_b1_pct": 10.0, "stft_b2_pct": -10.0, "ltft_b1_pct": 5.0})
    expected = 10 * 1.05 / 14.08 / 745  # banks average to 0 %, LTFT +5 %
    assert kernel.trip_metrics(kernel.clean_points(trims)).iloc[0]["fuel_l"] == pytest.approx(
        expected
    )


def test_clean_points_flags_out_of_area_fixes() -> None:
    frame = pd.DataFrame({"t_ms": [0, 1000, 2000], "speed_kmh": [-1.0, 10.0, 201.0],
                          "lat": [45.0, 42.3, 42.3], "lon": [-83.7, -83.7, None]})
    cleaned = kernel.clean_points(frame)
    assert cleaned["speed_out_of_range"].tolist() == [True, False, True]
    assert cleaned["speed_kmh"].isna().tolist() == [True, False, True]
    assert cleaned["gps_out_of_area"].tolist() == [True, False, False]  # half fix: not flagged
    assert cleaned["lat"].isna().tolist() == [True, False, True]


# --- Spark end to end ------------------------------------------------------------------
def envelope(vehicle_id: int, trip_id: int, points: list[dict[str, Any]], *,
             sent_at: datetime = ON_TIME, received_at: datetime = RECEIVED) -> str:
    payload = {
        "vehicle_id": vehicle_id,
        "trip_id": trip_id,
        "started_at": START.isoformat(),
        "sent_at": sent_at.isoformat(),
        "points": points,
    }
    return json.dumps({
        "record_id": f"r-{vehicle_id}-{trip_id}-{received_at:%H%M%S}-{sent_at:%d%H%M}",
        "received_at": received_at.isoformat(),
        "partner_id": "test",
        "content_sha256": content_sha256(payload),
        "payload": payload,
    })


def write_raw(root: Path) -> Path:
    folder = root / "trips" / "ingest_date=2026-11-17" / "partner=test"
    folder.mkdir(parents=True)
    full = make_points()
    first = [
        envelope(1, 1, make_points(**CASES["clean"])),
        envelope(1, 2, full),
        envelope(1, 2, full, sent_at=END + timedelta(hours=2),
                 received_at=RECEIVED + timedelta(hours=2)),  # exact resend
        envelope(1, 3, full[:61], sent_at=START + timedelta(seconds=60)),  # partial
        envelope(1, 3, full, received_at=RECEIVED + timedelta(minutes=1)),  # full version
        "this line is not JSON",
    ]
    second = [
        envelope(1, 4, make_points(**CASES["spike"])),
        envelope(1, 5, make_points(**CASES["gps_gap"])),
        envelope(1, 6, make_points(**CASES["gps_jump"])),
        envelope(1, 7, full, sent_at=END + timedelta(hours=30)),
        envelope(1, 8, make_points(**CASES["idle"])),
        envelope(2, 1, make_points(**CASES["ev"])),
        envelope(1, 9, make_points(dup_at=50)),
    ]
    (folder / "093000000000-a.jsonl").write_text("\n".join(first) + "\n")
    (folder / "093100000000-b.jsonl").write_text("\n".join(second) + "\n")
    return root / "trips"


@pytest.fixture(scope="module")
def curated(spark: Any, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    from vedpipe.spark import job

    base = tmp_path_factory.mktemp("ved")
    raw = write_raw(base / "raw")
    out = base / "curated"
    stats = job.run(spark, str(raw), str(out))
    trips = {(r.vehicle_id, r.trip_id): r for r in spark.read.parquet(str(out / "trips")).collect()}
    return {"stats": stats, "trips": trips, "raw": raw, "out": out}


@pytest.mark.spark
def test_ingest_stats(curated: dict[str, Any]) -> None:
    assert curated["stats"] == [{
        "ingest_date": date(2026, 11, 17), "n_records": 13, "n_corrupt": 1, "n_trips": 10,
        "n_exact_dup_records": 1, "n_superseded_records": 1,
    }]


@pytest.mark.spark
def test_clean_trip(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 1)]
    assert t.distance_km == pytest.approx(1.0)
    assert t.duration_s == 100
    assert t.fuel_l == pytest.approx(FUEL_L)
    assert t.fuel_kwh == pytest.approx(FUEL_L * kernel.FUEL_KWH_PER_L)
    assert t.has_fuel_data
    assert not t.has_batt_data
    assert t.n_points == 101
    assert t.delay_s == pytest.approx(300)
    assert t.trip_date == date(2026, 11, 16)  # 08:00 UTC = 03:00 in Detroit
    assert t.ingest_date == date(2026, 11, 17)
    # PySpark returns naive local-time datetimes; compare instants.
    assert t.ended_at.timestamp() == END.timestamp()
    assert not any([t.flag_missing_gps, t.flag_gps_jump, t.flag_impossible_speed,
                    t.flag_late, t.flag_duplicate])


@pytest.mark.spark
def test_exact_resend_collapses_to_first_copy(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 2)]
    assert (t.n_records, t.n_versions, t.n_exact_dups) == (2, 1, 1)
    assert t.flag_duplicate
    assert not t.flag_late
    assert t.delay_s == pytest.approx(300)  # measured on the first copy


@pytest.mark.spark
def test_full_version_supersedes_partial(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 3)]
    assert t.duration_s == 100
    assert t.n_points == 101
    assert (t.n_records, t.n_versions, t.n_exact_dups) == (2, 2, 0)
    assert not t.flag_duplicate


@pytest.mark.spark
def test_speed_spike(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 4)]
    assert t.n_speed_out_of_range == 1
    assert t.distance_km == pytest.approx(1.0)
    assert t.max_speed_kmh == 36
    assert t.flag_impossible_speed


@pytest.mark.spark
def test_gps_gap_and_jump(curated: dict[str, Any]) -> None:
    gap, jump = curated["trips"][(1, 5)], curated["trips"][(1, 6)]
    assert gap.max_gps_gap_s == 69
    assert (gap.flag_missing_gps, gap.flag_gps_jump) == (True, False)
    assert (jump.flag_gps_jump, jump.flag_missing_gps) == (True, False)


@pytest.mark.spark
def test_late_delivery(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 7)]
    assert t.delay_s == pytest.approx(108_000)
    assert t.flag_late


@pytest.mark.spark
def test_idle_time(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 8)]
    assert (t.idle_s, t.moving_s) == (20, 80)
    assert t.distance_km == pytest.approx(0.8)


@pytest.mark.spark
def test_ev_battery_energy(curated: dict[str, Any]) -> None:
    t = curated["trips"][(2, 1)]
    assert t.elec_kwh == pytest.approx(0.1944, abs=1e-4)
    assert t.energy_kwh == pytest.approx(EV_KWH)
    assert t.has_batt_data
    assert not t.has_fuel_data


@pytest.mark.spark
def test_duplicate_sample_dropped(curated: dict[str, Any]) -> None:
    t = curated["trips"][(1, 9)]
    assert t.n_dup_points == 1
    assert t.n_points == 101


@pytest.mark.spark
def test_trip_points_written(spark: Any, curated: dict[str, Any]) -> None:
    points = spark.read.parquet(str(curated["out"] / "trip_points"))
    assert points.count() == 101 * 10
    assert points.filter("speed_out_of_range").count() == 1


@pytest.mark.spark
def test_rerun_is_idempotent(spark: Any, curated: dict[str, Any]) -> None:
    from vedpipe.spark import job

    stats = job.run(spark, str(curated["raw"]), str(curated["out"]))
    assert stats == curated["stats"]
    assert spark.read.parquet(str(curated["out"] / "trips")).count() == 10
