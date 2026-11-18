"""VED weekly CSV -> contract v1.0 trip payloads.

What the data dictates:

* Rows are sparse: each signal has its own sampling period, so a row carries only the
  signals measured at that timestamp. Missing values are omitted from the payload,
  never zero-filled, and rows sharing a timestamp are merged.
* Headers vary in spelling and units across exports, so they are matched on a
  normalised form (lowercase, alphanumerics only), with a unit-less fallback for the
  unambiguous ones (AC power exists in kW *and* W, so it always needs its unit).
* ``DayNum`` is days since 2017-11-01 00:00 local time (DayNum 1.0). Whether it is
  constant per trip or advances per row is not documented, so both are handled: when
  it varies, the trip start is the DayNum at the smallest Timestamp minus that offset.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from vedpipe.schema import SIGNAL_FIELDS, normalise_name

log = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("America/Detroit")
DAYNUM_EPOCH = datetime(2017, 11, 1)  # local wall-clock time of DayNum == 1.0
MS_PER_DAY = 86_400_000
# Below ~9 ms of spread we treat DayNum as constant within a trip.
DAYNUM_CONSTANT_TOLERANCE_DAYS = 1e-7

KEY_COLUMNS = ("day_num", "vehicle_id", "trip_id", "t_ms")

# Local data directory layout shared by fetch and replay.
WEEK_FILE = re.compile(r"VED_(\d{6})_week\.csv$")
DYNAMIC_DIR = "dynamic"
MANIFEST = "fetch_manifest.json"
# Collection period with a margin: Nov 1, 2017 to Nov 9, 2018 per the VED paper.
COLLECTION_START = date(2017, 10, 1)
COLLECTION_END = date(2018, 12, 31)

# Normalised VED header (with units) -> canonical column name.
HEADER_ALIASES: dict[str, str] = {
    "daynum": "day_num",
    "vehid": "vehicle_id",
    "trip": "trip_id",
    "timestampms": "t_ms",
    "latitudedeg": "lat",
    "longitudedeg": "lon",
    "vehiclespeedkmh": "speed_kmh",
    "mafgsec": "maf_g_s",
    "enginerpmrpm": "engine_rpm",
    "absoluteload": "abs_load_pct",
    "oatdegc": "oat_c",
    "outsideairtemperaturedegc": "oat_c",
    "fuelratelhr": "fuel_rate_l_h",
    "airconditioningpowerkw": "ac_power_kw",
    "airconditioningpowerwatts": "ac_power_w",
    "heaterpowerwatts": "heater_power_w",
    "hvbatterycurrenta": "hv_current_a",
    "hvbatterysoc": "hv_soc_pct",
    "hvbatteryvoltagev": "hv_voltage_v",
    "shorttermfueltrimbank1": "stft_b1_pct",
    "shorttermfueltrimbank2": "stft_b2_pct",
    "longtermfueltrimbank1": "ltft_b1_pct",
    "longtermfueltrimbank2": "ltft_b2_pct",
}

# Unit-less fallbacks for headers whose meaning does not depend on the unit.
BARE_ALIASES: dict[str, str] = {
    "vehicleid": "vehicle_id",
    "tripid": "trip_id",
    "timestamp": "t_ms",
    "latitude": "lat",
    "longitude": "lon",
    "vehiclespeed": "speed_kmh",
    "maf": "maf_g_s",
    "enginerpm": "engine_rpm",
    "oat": "oat_c",
    "outsideairtemperature": "oat_c",
    "fuelrate": "fuel_rate_l_h",
    "heaterpower": "heater_power_w",
    "hvbatterycurrent": "hv_current_a",
    "hvbatteryvoltage": "hv_voltage_v",
}

_UNIT = re.compile(r"[\[\(].*?[\]\)]")


def week_date(name: str) -> date | None:
    """Start date of a weekly file ``VED_<6 digits>_week.csv``.

    The README documents the digits as mmddyy, while file names seen in the wild read
    as yymmdd (``VED_171101_week.csv``). Both are accepted: only one reading can fall
    inside the collection period, because 17/18 is never a valid month.
    """
    match = WEEK_FILE.search(name)
    if not match:
        return None
    for fmt in ("%y%m%d", "%m%d%y"):
        try:
            parsed = datetime.strptime(match.group(1), fmt).date()
        except ValueError:
            continue
        if COLLECTION_START <= parsed <= COLLECTION_END:
            return parsed
    return None


def map_header(name: str) -> str | None:
    """Canonical column for a VED header, or None when the column is not used."""
    full = normalise_name(name)
    if full in HEADER_ALIASES:
        return HEADER_ALIASES[full]
    return BARE_ALIASES.get(normalise_name(_UNIT.sub("", str(name))))


def map_columns(columns: Iterable[str]) -> dict[str, str]:
    """``{original: canonical}`` for the usable columns; fails if a key column is missing."""
    mapping: dict[str, str] = {}
    seen: set[str] = set()
    for column in columns:
        canonical = map_header(column)
        if canonical is None:
            log.debug("ignoring unknown VED column %r", column)
            continue
        if canonical in seen:
            log.warning("column %r duplicates %s; keeping the first", column, canonical)
            continue
        mapping[column] = canonical
        seen.add(canonical)
    missing = [c for c in KEY_COLUMNS if c not in seen]
    if missing:
        raise ValueError(f"VED file lacks required columns: {', '.join(missing)}")
    return mapping


def read_weekly_csv(path: Path) -> pd.DataFrame:
    """Read one weekly CSV with canonical, numeric columns."""
    frame = pd.read_csv(path, low_memory=False)
    mapping = map_columns(frame.columns)
    frame = frame[list(mapping)].rename(columns=mapping)
    for column in frame.columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["vehicle_id", "trip_id", "t_ms"])


def day_num_to_datetime(day_num: float) -> datetime:
    """DayNum -> timezone-aware local datetime, rounded to the millisecond."""
    offset_ms = round((day_num - 1.0) * MS_PER_DAY)
    return (DAYNUM_EPOCH + timedelta(milliseconds=offset_ms)).replace(tzinfo=LOCAL_TZ)


def trip_start(day_num: np.ndarray, t_ms: np.ndarray) -> datetime:
    """Trip start for both DayNum conventions (constant per trip, or advancing per row)."""
    day = np.asarray(day_num, dtype=float)
    t = np.asarray(t_ms, dtype=float)
    valid = ~np.isnan(day) & ~np.isnan(t)
    if not valid.any():
        raise ValueError("trip has no rows with both DayNum and Timestamp")
    day, t = day[valid], t[valid]
    if day.max() - day.min() <= DAYNUM_CONSTANT_TOLERANCE_DAYS:
        return day_num_to_datetime(float(day.min()))
    first = int(np.argmin(t))
    return day_num_to_datetime(float(day[first] - t[first] / MS_PER_DAY))


def build_payload(vehicle_id: int, trip_id: int, rows: pd.DataFrame) -> dict[str, Any] | None:
    """One trip payload; rows sharing a timestamp are merged, absent signals omitted."""
    rows = rows[rows["t_ms"] >= 0]
    if rows.empty:
        return None
    signals = [c for c in SIGNAL_FIELDS if c in rows.columns]
    merged = rows.groupby("t_ms", sort=True)[signals].first()
    values = merged.to_numpy(dtype=float)
    points: list[dict[str, Any]] = []
    for t, row in zip(merged.index, values, strict=True):
        point: dict[str, Any] = {"t": int(t)}
        for name, value in zip(signals, row, strict=True):
            if not math.isnan(value):
                point[name] = float(value)
        if len(point) > 1:
            points.append(point)
    if not points:
        return None
    start = trip_start(rows["day_num"].to_numpy(), rows["t_ms"].to_numpy())
    return {
        "vehicle_id": int(vehicle_id),
        "trip_id": int(trip_id),
        "started_at": start.isoformat(timespec="milliseconds"),
        "points": points,
    }


def iter_trip_payloads(csv_paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    """Payloads file by file, each file's trips ordered by start time.

    Weekly files cover consecutive weeks, so passing them in chronological order yields
    trips (almost) globally ordered by start, which the replay planner relies on.
    """
    for path in csv_paths:
        frame = read_weekly_csv(path)
        payloads = [
            payload
            for (vehicle_id, trip_id), rows in frame.groupby(["vehicle_id", "trip_id"], sort=False)
            if (payload := build_payload(int(vehicle_id), int(trip_id), rows)) is not None
        ]
        # Sort on the parsed instant: around the DST switch the ISO strings carry different
        # offsets and would not sort chronologically as text.
        payloads.sort(
            key=lambda p: (datetime.fromisoformat(p["started_at"]), p["vehicle_id"], p["trip_id"])
        )
        log.info("%s: %d trips from %d rows", path.name, len(payloads), len(frame))
        yield from payloads
