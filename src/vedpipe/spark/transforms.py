"""Spark transformations of the curation job: DataFrame in, DataFrame out, no I/O.

Record level (native Spark, window functions):
  * a line that does not parse, or lacks the trip key, start or points, is *corrupt*;
  * exact resends (same trip, same content hash) collapse to the first copy received,
    so lateness is measured on the original delivery;
  * among distinct versions of a trip the newest by partner ``sent_at`` wins (a full
    trip supersedes a partial one sent mid-trip).

Point and trip level: points are exploded, duplicate offsets dropped (and counted),
then the rules and integrals in :mod:`vedpipe.spark.kernel` run per batch / per trip.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, NamedTuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from vedpipe.schema import INGEST_STATS_COLUMNS, SIGNAL_FIELDS, TRIP_COLUMNS, TRIP_KEY, ColumnKind
from vedpipe.spark import kernel

if TYPE_CHECKING:
    import pandas as pd

LOCAL_TZ = "America/Detroit"
LATE_AFTER_S = 24 * 3600
GPS_GAP_MISSING_S = 60.0
COVERAGE_OK = 0.9

_SPARK_TYPES: dict[ColumnKind, T.DataType] = {
    "long": T.LongType(),
    "double": T.DoubleType(),
    "bool": T.BooleanType(),
    "string": T.StringType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
}


class CuratedOutputs(NamedTuple):
    trips: DataFrame
    ingest_stats: DataFrame
    trip_points: DataFrame


def struct_type(columns: tuple[tuple[str, ColumnKind], ...]) -> T.StructType:
    return T.StructType([T.StructField(name, _SPARK_TYPES[kind]) for name, kind in columns])


def _conform(df: DataFrame, columns: tuple[tuple[str, ColumnKind], ...]) -> DataFrame:
    """Select and cast to a contract from :mod:`vedpipe.schema`, in contract order."""
    return df.select(*[F.col(n).cast(_SPARK_TYPES[k]).alias(n) for n, k in columns])


def raw_schema() -> T.StructType:
    """Explicit read schema: no inference pass over the bucket, stable types.

    Timestamps are read as strings and cast later because partners send ISO-8601 with
    arbitrary offsets and sub-second precision, which a fixed reader format would
    reject as corrupt.
    """
    point = T.StructType(
        [T.StructField("t", T.LongType())]
        + [T.StructField(name, T.DoubleType()) for name in SIGNAL_FIELDS]
    )
    payload = T.StructType(
        [
            T.StructField("vehicle_id", T.LongType()),
            T.StructField("trip_id", T.LongType()),
            T.StructField("started_at", T.StringType()),
            T.StructField("sent_at", T.StringType()),
            T.StructField("points", T.ArrayType(point)),
        ]
    )
    return T.StructType(
        [
            T.StructField("record_id", T.StringType()),
            T.StructField("received_at", T.StringType()),
            T.StructField("partner_id", T.StringType()),
            T.StructField("content_sha256", T.StringType()),
            T.StructField("payload", payload),
            T.StructField("_corrupt_record", T.StringType()),
        ]
    )


def _plus_ms(ts_column: str, ms_column: str) -> Column:
    """Timestamp + milliseconds at microsecond precision (no float rounding)."""
    return F.expr(f"timestamp_micros(unix_micros({ts_column}) + {ms_column} * 1000)")


def parse_records(raw: DataFrame) -> DataFrame:
    """Flatten envelopes; ``ingest_date`` comes from the object path, not the payload."""
    payload = F.col("payload")
    started_at = payload["started_at"].cast("timestamp")
    received_at = F.col("received_at").cast("timestamp")
    is_corrupt = (
        F.col("_corrupt_record").isNotNull()
        | payload.isNull()
        | payload["vehicle_id"].isNull()
        | payload["trip_id"].isNull()
        | started_at.isNull()
        | received_at.isNull()
        | F.col("content_sha256").isNull()
        | (F.coalesce(F.size(payload["points"]), F.lit(-1)) < 1)
    )
    ingest_date = F.regexp_extract("source_file", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    return raw.select(
        F.to_date(F.when(ingest_date != "", ingest_date)).alias("ingest_date"),
        F.col("record_id"),
        F.col("partner_id"),
        F.col("content_sha256"),
        received_at.alias("received_at"),
        payload["vehicle_id"].alias("vehicle_id"),
        payload["trip_id"].alias("trip_id"),
        started_at.alias("started_at"),
        payload["sent_at"].cast("timestamp").alias("sent_at"),
        payload["points"].alias("points"),
        is_corrupt.alias("is_corrupt"),
    )


def classify_records(records: DataFrame) -> DataFrame:
    """Add ``is_exact_dup``, ``is_superseded`` and ``is_winner`` to every record."""
    ok = ~F.col("is_corrupt")
    copy_order = Window.partitionBy(*TRIP_KEY, "content_sha256").orderBy(
        "received_at", "record_id"
    )
    ranked = records.withColumn("_copy", F.row_number().over(copy_order))
    is_first_copy = ok & (F.col("_copy") == 1)
    # First copies sort before resends, so row_number ranks versions among them only.
    version_order = Window.partitionBy(*TRIP_KEY).orderBy(
        is_first_copy.desc(),
        F.coalesce("sent_at", "received_at").desc(),
        F.col("received_at").desc(),
        F.col("record_id").desc(),
    )
    ranked = ranked.withColumn("_version", F.row_number().over(version_order))
    return ranked.select(
        *records.columns,
        (ok & (F.col("_copy") > 1)).alias("is_exact_dup"),
        (is_first_copy & (F.col("_version") > 1)).alias("is_superseded"),
        (is_first_copy & (F.col("_version") == 1)).alias("is_winner"),
    )


def ingest_stats(classified: DataFrame) -> DataFrame:
    """Per ingest day: records, corrupt, distinct trips, exact-dup and superseded records."""
    ok = ~F.col("is_corrupt")
    stats = classified.groupBy("ingest_date").agg(
        F.count(F.lit(1)).alias("n_records"),
        F.count(F.when(F.col("is_corrupt"), 1)).alias("n_corrupt"),
        F.countDistinct(F.when(ok, F.col("vehicle_id")), F.when(ok, F.col("trip_id"))).alias(
            "n_trips"
        ),
        F.count(F.when(F.col("is_exact_dup"), 1)).alias("n_exact_dup_records"),
        F.count(F.when(F.col("is_superseded"), 1)).alias("n_superseded_records"),
    )
    return _conform(stats, INGEST_STATS_COLUMNS)


def winning_versions(classified: DataFrame) -> DataFrame:
    """One row per trip: the winning record plus delivery counts and the local trip date."""
    per_trip = Window.partitionBy(*TRIP_KEY)
    counted = classified.withColumn(
        "n_records", F.count(F.when(~F.col("is_corrupt"), 1)).over(per_trip)
    ).withColumn("n_exact_dups", F.count(F.when(F.col("is_exact_dup"), 1)).over(per_trip))
    return counted.filter(F.col("is_winner")).select(
        *TRIP_KEY,
        "partner_id",
        "ingest_date",
        "started_at",
        "sent_at",
        "received_at",
        "points",
        "n_records",
        (F.col("n_records") - F.col("n_exact_dups")).alias("n_versions"),
        "n_exact_dups",
        F.to_date(F.from_utc_timestamp("started_at", LOCAL_TZ)).alias("trip_date"),
    )


def explode_points(winners: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Points of winning versions with absolute ``ts``; duplicate offsets keep the first.

    Returns ``(points, dup_counts)`` where ``dup_counts`` has ``n_dup_points`` per trip.
    """
    exploded = winners.select(
        *TRIP_KEY, "trip_date", "started_at", F.posexplode("points").alias("pos", "p")
    )
    flat = exploded.select(
        *TRIP_KEY,
        "trip_date",
        "pos",
        F.col("p.t").alias("t_ms"),
        _plus_ms("started_at", "p.t").alias("ts"),
        *[F.col(f"p.{name}").alias(name) for name in SIGNAL_FIELDS],
    ).filter(F.col("t_ms").isNotNull() & (F.col("t_ms") >= 0))
    first_at_offset = Window.partitionBy(*TRIP_KEY, "t_ms").orderBy("pos")
    ranked = flat.withColumn("_dup_rank", F.row_number().over(first_at_offset))
    dup_counts = ranked.groupBy(*TRIP_KEY).agg(
        F.count(F.when(F.col("_dup_rank") > 1, 1)).alias("n_dup_points")
    )
    points = ranked.filter(F.col("_dup_rank") == 1).drop("_dup_rank", "pos")
    return points, dup_counts


def _clean_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch in batches:
        yield kernel.clean_points(batch)


def clean_points(points: DataFrame) -> DataFrame:
    """Row-level rules (range, geofence) via the shared pandas kernel."""
    schema = T.StructType(
        [
            *points.schema.fields,
            T.StructField("speed_out_of_range", T.BooleanType()),
            T.StructField("gps_out_of_area", T.BooleanType()),
        ]
    )
    return points.mapInPandas(_clean_batches, schema=schema)


def trip_metrics(cleaned_points: DataFrame) -> DataFrame:
    """Per-trip integration and sequential checks; one kernel call per trip."""
    return (
        cleaned_points.select(*kernel.KERNEL_INPUT_COLUMNS)
        .groupBy(*TRIP_KEY)
        .applyInPandas(kernel.trip_metrics, schema=struct_type(kernel.METRIC_COLUMNS))
    )


def assemble_trips(winners: DataFrame, metrics: DataFrame, dup_counts: DataFrame) -> DataFrame:
    """Join delivery metadata and metrics; derive energy, delay and DQ flags."""
    trips = (
        winners.drop("points")
        .join(metrics, list(TRIP_KEY))
        .join(dup_counts, list(TRIP_KEY), "left")
        .withColumn("ended_at", _plus_ms("started_at", "t_max_ms"))
    )
    fuel_kwh = F.col("fuel_l") * F.lit(kernel.FUEL_KWH_PER_L)
    delay_s = F.expr(
        "(unix_micros(coalesce(sent_at, received_at)) - unix_micros(ended_at)) / 1e6"
    )
    trips = (
        trips.withColumn("fuel_kwh", fuel_kwh)
        .withColumn("energy_kwh", fuel_kwh + F.col("elec_kwh"))
        .withColumn("has_fuel_data", F.col("fuel_coverage") >= COVERAGE_OK)
        .withColumn("has_batt_data", F.col("batt_coverage") >= COVERAGE_OK)
        .withColumn("n_dup_points", F.coalesce("n_dup_points", F.lit(0)))
        .withColumn("delay_s", delay_s)
        .withColumn(
            "flag_missing_gps",
            (F.col("n_gps_fixes") == 0)
            | (F.coalesce("max_gps_gap_s", F.lit(0.0)) > GPS_GAP_MISSING_S),
        )
        .withColumn(
            "flag_gps_jump", (F.col("n_gps_jumps") > 0) | (F.col("n_gps_out_of_area") > 0)
        )
        .withColumn(
            "flag_impossible_speed",
            (F.col("n_speed_out_of_range") > 0) | (F.col("n_accel_spikes") > 0),
        )
        .withColumn("flag_late", F.col("delay_s") > LATE_AFTER_S)
        .withColumn("flag_duplicate", F.col("n_exact_dups") > 0)
    )
    return _conform(trips, TRIP_COLUMNS)


def build_outputs(raw: DataFrame) -> CuratedOutputs:
    """The whole curation as one lazy plan. ``raw`` needs a ``source_file`` column."""
    classified = classify_records(parse_records(raw))
    winners = winning_versions(classified)
    points, dup_counts = explode_points(winners)
    trip_points = clean_points(points)
    trips = assemble_trips(winners, trip_metrics(trip_points), dup_counts)
    return CuratedOutputs(trips=trips, ingest_stats=ingest_stats(classified),
                          trip_points=trip_points)
