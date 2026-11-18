"""Per-point rules and per-trip integration: pure pandas/numpy, no Spark import.

Spark runs these functions through ``mapInPandas`` (row rules) and
``groupBy().applyInPandas`` (one call per trip). Keeping them free of Spark means every
DQ rule and every integral has exactly one implementation, which unit tests can run
without a JVM. The cost is an Arrow round trip per batch; for VED-sized data (tens of
millions of points) that is minutes on a laptop, and a trip always fits in memory.

Conventions that come from the data:

* Signals are sampled at different rates (GPS 3 s, speed 1 s, MAF 2 s, fuel rate 5 s,
  battery current 1 s, voltage 5 s, long-term trims 30 s). Each signal is carried
  forward for a bounded time (zero-order hold) so a 1 Hz speed step can be paired with
  the last voltage, but a dropout never becomes a fabricated plateau.
* Integration is left Riemann: the step from t[i-1] to t[i] uses the values held at
  t[i-1]. Steps longer than 10 s are gaps and are not integrated.
* Battery current is positive when charging, so energy *out* of the pack is -V*I*dt.
* Fuel follows Algorithm 1 of the VED paper: Fuel Rate when present, else
  MAF * (1 + STFT/100 + LTFT/100) / AFR with banks averaged, AFR 14.08 (E10).
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

from vedpipe.schema import ColumnKind

# --- constants -----------------------------------------------------------------------
AFR_E10: Final = 14.08
FUEL_DENSITY_G_PER_L: Final = 745.0
# EPA MPGe convention: 33.7 kWh per US gallon of gasoline = 8.90 kWh/L.
FUEL_KWH_PER_L: Final = 33.7 / 3.785411784

SPEED_MIN_KMH: Final = 0.0
SPEED_MAX_KMH: Final = 200.0
ACCEL_SPIKE_M_S2: Final = 15.0
ACCEL_MIN_DT_S: Final = 0.5
ACCEL_MAX_DT_S: Final = 5.0
# Geofence of the published data (Ann Arbor area), lat/lon in degrees.
GPS_LAT_RANGE: Final = (41.9, 42.6)
GPS_LON_RANGE: Final = (-84.2, -83.3)
GPS_JUMP_KMH: Final = 250.0
GPS_JUMP_MIN_KM: Final = 0.1
EARTH_RADIUS_KM: Final = 6371.0088

MAX_STEP_S: Final = 10.0
IDLE_BELOW_KMH: Final = 1.0

# Zero-order-hold horizon per integrated signal, seconds (~2-3x its sampling period).
HOLD_S: Final[dict[str, float]] = {
    "speed_kmh": 3.0,
    "maf_g_s": 5.0,
    "fuel_rate_l_h": 10.0,
    "hv_current_a": 3.0,
    "hv_voltage_v": 10.0,
    "stft_b1_pct": 60.0,
    "stft_b2_pct": 60.0,
    "ltft_b1_pct": 60.0,
    "ltft_b2_pct": 60.0,
}

KERNEL_INPUT_COLUMNS: Final[tuple[str, ...]] = (
    "vehicle_id",
    "trip_id",
    "t_ms",
    *HOLD_S,
    "lat",
    "lon",
    "speed_out_of_range",
    "gps_out_of_area",
)

METRIC_COLUMNS: Final[tuple[tuple[str, ColumnKind], ...]] = (
    ("vehicle_id", "long"),
    ("trip_id", "long"),
    ("t_min_ms", "long"),
    ("t_max_ms", "long"),
    ("duration_s", "double"),
    ("distance_km", "double"),
    ("moving_s", "double"),
    ("idle_s", "double"),
    ("gap_s", "double"),
    ("max_speed_kmh", "double"),
    ("fuel_l", "double"),
    ("elec_kwh", "double"),
    ("fuel_coverage", "double"),
    ("batt_coverage", "double"),
    ("n_points", "long"),
    ("n_speed_out_of_range", "long"),
    ("n_accel_spikes", "long"),
    ("n_gps_fixes", "long"),
    ("n_gps_out_of_area", "long"),
    ("n_gps_jumps", "long"),
    ("max_gps_gap_s", "double"),
)
METRIC_NAMES: Final[list[str]] = [name for name, _ in METRIC_COLUMNS]


# --- row-level rules -------------------------------------------------------------------
def _series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Numeric column, or all-NaN when a partner never sent the signal."""
    if name not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").astype(float)


def clean_points(points: pd.DataFrame) -> pd.DataFrame:
    """Null out values that cannot be real and flag them; every other column passes through.

    * speed outside 0-200 km/h -> speed nulled, ``speed_out_of_range`` set
    * fix outside the published geofence -> lat/lon nulled, ``gps_out_of_area`` set
    * half a fix (lat without lon or vice versa) -> both nulled, not flagged
    """
    out = points.copy()
    speed = _series(out, "speed_kmh")
    out_of_range = speed.notna() & ((speed < SPEED_MIN_KMH) | (speed > SPEED_MAX_KMH))
    out["speed_kmh"] = speed.mask(out_of_range)

    lat, lon = _series(out, "lat"), _series(out, "lon")
    has_fix = lat.notna() & lon.notna()
    in_area = lat.between(*GPS_LAT_RANGE) & lon.between(*GPS_LON_RANGE)
    keep = has_fix & in_area
    out["lat"] = lat.where(keep)
    out["lon"] = lon.where(keep)

    out["speed_out_of_range"] = out_of_range.astype(bool)
    out["gps_out_of_area"] = (has_fix & ~in_area).astype(bool)
    return out


# --- per-trip integration --------------------------------------------------------------
def hold(t_s: np.ndarray, values: np.ndarray, horizon_s: float) -> np.ndarray:
    """Zero-order hold: last valid value at or before each t, if at most ``horizon_s`` old."""
    n = len(values)
    last = np.maximum.accumulate(np.where(~np.isnan(values), np.arange(n), -1))
    held = np.full(n, np.nan)
    idx = np.flatnonzero(last >= 0)
    fresh = (t_s[idx] - t_s[last[idx]]) <= horizon_s
    held[idx[fresh]] = values[last[idx[fresh]]]
    return held


def _bank_mean(bank1: np.ndarray, bank2: np.ndarray) -> np.ndarray:
    """Mean of the banks that are present (bank 2 exists only on V-engines)."""
    stacked = np.vstack([bank1, bank2])
    count = (~np.isnan(stacked)).sum(axis=0)
    total = np.nansum(stacked, axis=0)
    return np.where(count > 0, total / np.maximum(count, 1), np.nan)


def fuel_rate_l_h(held: dict[str, np.ndarray]) -> np.ndarray:
    """VED Algorithm 1: Fuel Rate if present, else trim-corrected MAF / AFR, in L/h.

    Missing trims count as 0 % (no correction) rather than discarding the MAF sample.
    """
    stft = np.nan_to_num(_bank_mean(held["stft_b1_pct"], held["stft_b2_pct"]), nan=0.0)
    ltft = np.nan_to_num(_bank_mean(held["ltft_b1_pct"], held["ltft_b2_pct"]), nan=0.0)
    fuel_g_s = held["maf_g_s"] * (1.0 + stft / 100.0 + ltft / 100.0) / AFR_E10
    from_maf = fuel_g_s / FUEL_DENSITY_G_PER_L * 3600.0
    direct = held["fuel_rate_l_h"]
    return np.where(~np.isnan(direct), direct, from_maf)


def haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray,
                 lon2: np.ndarray) -> np.ndarray:
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi, dlmb = phi2 - phi1, np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _column(frame: pd.DataFrame, name: str) -> np.ndarray:
    return _series(frame, name).to_numpy(dtype=float)


def _flag_count(frame: pd.DataFrame, name: str) -> int:
    return int(frame[name].eq(True).sum()) if name in frame.columns else 0


def trip_metrics(points: pd.DataFrame) -> pd.DataFrame:
    """One output row of :data:`METRIC_COLUMNS` for one trip of cleaned points.

    Precondition: ``t_ms`` is unique within the trip (duplicates are dropped upstream
    and counted there). Rows may arrive in any order.
    """
    if points.empty:
        return pd.DataFrame(columns=METRIC_NAMES)
    frame = points.sort_values("t_ms", kind="stable")
    t_ms = frame["t_ms"].to_numpy(dtype=np.int64)
    t = t_ms / 1000.0
    dt = np.diff(t)
    step = dt <= MAX_STEP_S
    duration_s = float(t[-1] - t[0])

    held = {name: hold(t, _column(frame, name), horizon) for name, horizon in HOLD_S.items()}

    def integrate(rate_per_s: np.ndarray) -> tuple[float, float]:
        """(integral, covered seconds) of a per-second rate over non-gap steps."""
        prev = rate_per_s[:-1]
        covered = step & ~np.isnan(prev)
        return float(np.sum(prev[covered] * dt[covered])), float(np.sum(dt[covered]))

    speed_held = held["speed_kmh"]
    distance_m, _ = integrate(speed_held / 3.6)
    prev_speed = speed_held[:-1]
    has_speed = step & ~np.isnan(prev_speed)
    idle_s = float(np.sum(dt[has_speed & (prev_speed < IDLE_BELOW_KMH)]))
    moving_s = float(np.sum(dt[has_speed & (prev_speed >= IDLE_BELOW_KMH)]))

    fuel_l, fuel_covered_s = integrate(fuel_rate_l_h(held) / 3600.0)
    battery_out_w = -held["hv_voltage_v"] * held["hv_current_a"]
    battery_j, batt_covered_s = integrate(battery_out_w)

    # Speed plausibility between *valid* samples (out-of-range values are already null).
    speed = _column(frame, "speed_kmh")
    valid = ~np.isnan(speed)
    v_t, v_kmh = t[valid], speed[valid]
    v_dt = np.diff(v_t)
    in_window = (v_dt >= ACCEL_MIN_DT_S) & (v_dt <= ACCEL_MAX_DT_S)
    accel = np.divide(np.diff(v_kmh) / 3.6, v_dt, out=np.zeros_like(v_dt), where=in_window)
    n_accel_spikes = int(np.sum(in_window & (np.abs(accel) > ACCEL_SPIKE_M_S2)))

    # GPS: interior gaps and implausible jumps between consecutive valid fixes.
    lat, lon = _column(frame, "lat"), _column(frame, "lon")
    fix = ~np.isnan(lat) & ~np.isnan(lon)
    f_t, f_lat, f_lon = t[fix], lat[fix], lon[fix]
    max_gps_gap_s = float("nan")
    n_gps_jumps = 0
    if len(f_t) >= 2:
        f_dt = np.diff(f_t)
        max_gps_gap_s = float(f_dt.max())
        dist_km = haversine_km(f_lat[:-1], f_lon[:-1], f_lat[1:], f_lon[1:])
        implied_kmh = np.divide(dist_km, f_dt / 3600.0, out=np.full_like(dist_km, np.inf),
                                where=f_dt > 0)
        n_gps_jumps = int(np.sum((dist_km > GPS_JUMP_MIN_KM) & (implied_kmh > GPS_JUMP_KMH)))

    def share(seconds: float) -> float:
        return seconds / duration_s if duration_s > 0 else 0.0

    row = {
        "vehicle_id": int(frame["vehicle_id"].iloc[0]),
        "trip_id": int(frame["trip_id"].iloc[0]),
        "t_min_ms": int(t_ms[0]),
        "t_max_ms": int(t_ms[-1]),
        "duration_s": duration_s,
        "distance_km": distance_m / 1000.0,
        "moving_s": moving_s,
        "idle_s": idle_s,
        "gap_s": float(np.sum(dt[~step])),
        "max_speed_kmh": float(v_kmh.max()) if len(v_kmh) else float("nan"),
        "fuel_l": fuel_l,
        "elec_kwh": battery_j / 3.6e6,
        "fuel_coverage": share(fuel_covered_s),
        "batt_coverage": share(batt_covered_s),
        "n_points": len(frame),
        "n_speed_out_of_range": _flag_count(frame, "speed_out_of_range"),
        "n_accel_spikes": n_accel_spikes,
        "n_gps_fixes": int(fix.sum()),
        "n_gps_out_of_area": _flag_count(frame, "gps_out_of_area"),
        "n_gps_jumps": n_gps_jumps,
        "max_gps_gap_s": max_gps_gap_s,
    }
    return pd.DataFrame([row], columns=METRIC_NAMES)
