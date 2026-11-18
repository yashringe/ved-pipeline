"""Column contracts shared by every layer.

The API model, the Spark schemas, the loader's COPY column list and a test against
``sql/001_schema.sql`` are all derived from these tuples. Adding a signal or a trip
column therefore happens in one place, and drift between layers fails a test instead
of silently dropping data.
"""

from __future__ import annotations

import re
from typing import Final, Literal

ColumnKind = Literal["long", "double", "bool", "string", "timestamp", "date"]

# Point-level signals in contract v1.0, in VED column order. Units are part of the name
# so nobody has to guess whether a value is W or kW.
SIGNAL_FIELDS: Final[tuple[str, ...]] = (
    "lat",
    "lon",
    "speed_kmh",
    "maf_g_s",
    "engine_rpm",
    "abs_load_pct",
    "oat_c",
    "fuel_rate_l_h",
    "ac_power_kw",
    "ac_power_w",
    "heater_power_w",
    "hv_current_a",
    "hv_soc_pct",
    "hv_voltage_v",
    "stft_b1_pct",
    "stft_b2_pct",
    "ltft_b1_pct",
    "ltft_b2_pct",
)

TRIP_KEY: Final[tuple[str, str]] = ("vehicle_id", "trip_id")

# One row per (vehicle_id, trip_id) in curated/trips and ved.fact_trip, in this order.
TRIP_COLUMNS: Final[tuple[tuple[str, ColumnKind], ...]] = (
    ("vehicle_id", "long"),
    ("trip_id", "long"),
    ("partner_id", "string"),
    ("started_at", "timestamp"),
    ("ended_at", "timestamp"),
    ("trip_date", "date"),
    ("ingest_date", "date"),
    ("duration_s", "double"),
    ("distance_km", "double"),
    ("moving_s", "double"),
    ("idle_s", "double"),
    ("gap_s", "double"),
    ("max_speed_kmh", "double"),
    ("fuel_l", "double"),
    ("fuel_kwh", "double"),
    ("elec_kwh", "double"),
    ("energy_kwh", "double"),
    ("fuel_coverage", "double"),
    ("batt_coverage", "double"),
    ("has_fuel_data", "bool"),
    ("has_batt_data", "bool"),
    ("n_points", "long"),
    ("n_dup_points", "long"),
    ("n_speed_out_of_range", "long"),
    ("n_accel_spikes", "long"),
    ("n_gps_fixes", "long"),
    ("n_gps_out_of_area", "long"),
    ("n_gps_jumps", "long"),
    ("max_gps_gap_s", "double"),
    ("n_records", "long"),
    ("n_versions", "long"),
    ("n_exact_dups", "long"),
    ("sent_at", "timestamp"),
    ("received_at", "timestamp"),
    ("delay_s", "double"),
    ("flag_missing_gps", "bool"),
    ("flag_gps_jump", "bool"),
    ("flag_impossible_speed", "bool"),
    ("flag_late", "bool"),
    ("flag_duplicate", "bool"),
)

# One row per ingest_date in curated/ingest_stats and ved.dq_ingest_daily.
INGEST_STATS_COLUMNS: Final[tuple[tuple[str, ColumnKind], ...]] = (
    ("ingest_date", "date"),
    ("n_records", "long"),
    ("n_corrupt", "long"),
    ("n_trips", "long"),
    ("n_exact_dup_records", "long"),
    ("n_superseded_records", "long"),
)


def column_names(columns: tuple[tuple[str, ColumnKind], ...]) -> list[str]:
    """Names only, in contract order (used for COPY and SELECT lists)."""
    return [name for name, _ in columns]


def normalise_name(name: object) -> str:
    """Lowercase alphanumerics only: how VED headers are matched across exports."""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())
