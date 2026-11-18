"""Load curated Parquet and the VED static vehicle data into PostgreSQL.

One run = one transaction: apply ``sql/*.sql`` (idempotent DDL and views), upsert
vehicles, COPY trips into an unlogged staging table and upsert them into
``ved.fact_trip``, and replace the ingest statistics. Readers therefore see either the
previous load or the new one, never a half-loaded warehouse.

Driver imports (psycopg, pyarrow) live inside the I/O functions so the pure static-data
normalisation can be imported and tested without database or Arrow libraries.
"""

from __future__ import annotations

import argparse
import logging
import math
import re
from collections.abc import Iterator, Sequence
from dataclasses import astuple, dataclass, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import pandas as pd

from vedpipe.config import Settings, setup_logging
from vedpipe.schema import (
    INGEST_STATS_COLUMNS,
    TRIP_COLUMNS,
    TRIP_KEY,
    column_names,
    normalise_name,
)

if TYPE_CHECKING:
    import psycopg
    import pyarrow as pa
    import pyarrow.fs as pafs

log = logging.getLogger(__name__)

STATIC_GLOB = "VED_Static_Data_*.xlsx"
NO_DATA = frozenset({"", "nodata", "na", "nan", "none", "null"})
_NUMBER = re.compile(r"-?\d[\d,]*(?:\.\d+)?")

# Normalised header prefix -> Vehicle field. Prefixes tolerate unit suffixes and the
# "Engine Configuration & Displacement" spelling variants.
STATIC_HEADERS: dict[str, str] = {
    "vehid": "vehicle_id",
    "enginetype": "engine_type_raw",
    "vehicleclass": "vehicle_class",
    "engineconfiguration": "engine_config",
    "transmission": "transmission",
    "drivewheels": "drive_wheels",
    "generalizedweight": "weight_lb",
}


@dataclass(frozen=True)
class Vehicle:
    """One row of ved.dim_vehicle; field order matches the INSERT column list."""

    vehicle_id: int
    vehicle_type: str
    engine_type_raw: str | None
    vehicle_class: str | None
    engine_config: str | None
    transmission: str | None
    drive_wheels: str | None
    weight_lb: int | None
    source_file: str


VEHICLE_COLUMNS = [f.name for f in fields(Vehicle)]


def clean_text(value: Any) -> str | None:
    """Trimmed text, or None for blanks, NaN and VED's "NO DATA" marker."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return None if normalise_name(text) in NO_DATA else text


def to_int(value: Any) -> int | None:
    """Integer from numbers or text like "3,500" or "3500 lb"; None when not numeric."""
    text = clean_text(value)
    match = _NUMBER.search(text) if text is not None else None
    return round(float(match.group().replace(",", ""))) if match else None


def map_vehicle_type(engine_type: str | None) -> str:
    """Normalise VED EngineType to ICE / HEV / PHEV / EV (order matters: PHEV > HEV > EV)."""
    code = normalise_name(engine_type or "").upper()
    if "PHEV" in code or "PLUGIN" in code:
        return "PHEV"
    if "HEV" in code or "HYBRID" in code:
        return "HEV"
    if code in {"EV", "BEV"} or "ELECTRIC" in code:
        return "EV"
    if "ICE" in code or "GAS" in code:
        return "ICE"
    return "UNKNOWN"


def _static_field(header: Any) -> str | None:
    name = normalise_name(header)
    return next((f for prefix, f in STATIC_HEADERS.items() if name.startswith(prefix)), None)


def normalise_static(frame: pd.DataFrame, source_file: str) -> list[Vehicle]:
    """Vehicles from one static sheet; rows without a numeric VehId are skipped."""
    mapping: dict[str, str] = {}
    for column in frame.columns:
        target = _static_field(column)
        if target and target not in mapping.values():
            mapping[column] = target
    if "vehicle_id" not in mapping.values():
        raise ValueError(f"{source_file}: no VehId column in {list(frame.columns)}")
    renamed = frame[list(mapping)].rename(columns=mapping)
    vehicles = []
    for row in renamed.to_dict("records"):
        vehicle_id = to_int(row.get("vehicle_id"))
        if vehicle_id is None:
            continue
        engine_type = clean_text(row.get("engine_type_raw"))
        vehicles.append(
            Vehicle(
                vehicle_id=vehicle_id,
                vehicle_type=map_vehicle_type(engine_type),
                engine_type_raw=engine_type,
                vehicle_class=clean_text(row.get("vehicle_class")),
                engine_config=clean_text(row.get("engine_config")),
                transmission=clean_text(row.get("transmission")),
                drive_wheels=clean_text(row.get("drive_wheels")),
                weight_lb=to_int(row.get("weight_lb")),
                source_file=source_file,
            )
        )
    return vehicles


def read_static(data_dir: Path) -> list[Vehicle]:
    """All static sheets in ``data_dir``; a VehId listed twice keeps the last file's row."""
    by_id: dict[int, Vehicle] = {}
    for path in sorted(data_dir.glob(STATIC_GLOB)):
        for vehicle in normalise_static(pd.read_excel(path, dtype=object), path.name):
            if vehicle.vehicle_id in by_id:
                log.warning("vehicle %d listed twice; using %s", vehicle.vehicle_id, path.name)
            by_id[vehicle.vehicle_id] = vehicle
    if not by_id:
        log.warning("no static vehicle data in %s: trips will load as UNKNOWN type", data_dir)
    return list(by_id.values())


# --- SQL ---------------------------------------------------------------------------------
# Identifiers below come from vedpipe.schema constants, never from input.
def upsert_vehicles_sql() -> str:
    cols = ", ".join(VEHICLE_COLUMNS)
    values = ", ".join(["%s"] * len(VEHICLE_COLUMNS))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in VEHICLE_COLUMNS if c != "vehicle_id")
    return (
        f"INSERT INTO ved.dim_vehicle ({cols}) VALUES ({values}) "
        f"ON CONFLICT (vehicle_id) DO UPDATE SET {updates}, loaded_at = now()"
    )


def copy_trips_sql() -> str:
    return f"COPY ved.stg_trip ({', '.join(column_names(TRIP_COLUMNS))}) FROM STDIN"


def upsert_trips_sql() -> str:
    """Staging -> fact. Upsert (not truncate+insert) keeps trips whose raw data aged out."""
    names = column_names(TRIP_COLUMNS)
    cols = ", ".join(names)
    updates = ", ".join(f"{n} = EXCLUDED.{n}" for n in names if n not in TRIP_KEY)
    return (
        f"INSERT INTO ved.fact_trip ({cols}) SELECT {cols} FROM ved.stg_trip "
        f"ON CONFLICT ({', '.join(TRIP_KEY)}) DO UPDATE SET {updates}, loaded_at = now()"
    )


def insert_stats_sql() -> str:
    names = column_names(INGEST_STATS_COLUMNS)
    return (
        f"INSERT INTO ved.dq_ingest_daily ({', '.join(names)}) "
        f"VALUES ({', '.join(['%s'] * len(names))})"
    )


# --- I/O -----------------------------------------------------------------------------------
def curated_filesystem(curated_uri: str, settings: Settings) -> tuple[pafs.FileSystem, str]:
    """pyarrow filesystem and base path for ``s3a://bucket`` or a local directory."""
    import pyarrow.fs as pafs

    if curated_uri.startswith(("s3a://", "s3://")):
        endpoint = urlparse(settings.s3_endpoint_url)
        fs = pafs.S3FileSystem(
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            endpoint_override=endpoint.netloc,
            scheme=endpoint.scheme or "http",
            region=settings.s3_region,
        )
        return fs, curated_uri.split("://", 1)[1].rstrip("/")
    return pafs.LocalFileSystem(), str(Path(curated_uri).resolve())


def read_parquet(fs: pafs.FileSystem, path: str, date_partition: str | None = None) -> pa.Table:
    """Read a Spark Parquet folder; the hive partition column is typed as a date."""
    import pyarrow as pa
    import pyarrow.dataset as ds

    partitioning = (
        ds.partitioning(pa.schema([(date_partition, pa.date32())]), flavor="hive")
        if date_partition
        else None
    )
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning=partitioning).to_table()


def table_rows(table: pa.Table, names: Sequence[str]) -> Iterator[tuple[Any, ...]]:
    columns = [table.column(name).to_pylist() for name in names]
    return zip(*columns, strict=True)


def load(
    conn: psycopg.Connection,
    sql_dir: Path,
    vehicles: Sequence[Vehicle],
    trips: pa.Table,
    stats: pa.Table,
) -> dict[str, int]:
    """Everything in the caller's transaction; commit happens when the caller exits."""
    with conn.cursor() as cur:
        cur.execute("SET LOCAL client_min_messages = warning")
        for path in sorted(sql_dir.glob("*.sql")):
            cur.execute(path.read_text())
        if vehicles:
            cur.executemany(upsert_vehicles_sql(), [astuple(v) for v in vehicles])
        with cur.copy(copy_trips_sql()) as copy:
            for row in table_rows(trips, column_names(TRIP_COLUMNS)):
                copy.write_row(row)
        cur.execute(upsert_trips_sql())
        upserted = cur.rowcount
        cur.execute("TRUNCATE ved.stg_trip")
        # Ingest stats are a full recompute in Spark, so replace them wholesale.
        cur.execute("DELETE FROM ved.dq_ingest_daily")
        stat_rows = list(table_rows(stats, column_names(INGEST_STATS_COLUMNS)))
        if stat_rows:
            cur.executemany(insert_stats_sql(), stat_rows)
    return {"vehicles": len(vehicles), "trips": upserted, "ingest_days": len(stat_rows)}


def main(argv: list[str] | None = None) -> None:
    import psycopg

    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--curated", default=f"s3a://{settings.curated_bucket}")
    parser.add_argument("--data-dir", type=Path, default=settings.ved_data_dir)
    parser.add_argument("--sql-dir", type=Path, default=settings.sql_dir)
    args = parser.parse_args(argv)
    setup_logging()

    vehicles = read_static(args.data_dir)
    fs, base = curated_filesystem(args.curated, settings)
    trips = read_parquet(fs, f"{base}/trips", date_partition="trip_date")
    stats = read_parquet(fs, f"{base}/ingest_stats")
    with psycopg.connect(settings.pg_dsn) as conn:
        counts = load(conn, args.sql_dir, vehicles, trips, stats)
    log.info("load committed: %s", counts)


if __name__ == "__main__":
    main()
