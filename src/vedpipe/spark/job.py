"""Curation job entry point: the only module that does I/O.

Every run recomputes all curated outputs from the full raw bucket and overwrites them.
That makes the job idempotent and lets a rule change apply to history without a
backfill procedure; the price is runtime that grows with raw volume (see README,
"Next steps": incremental MERGE into Iceberg/Delta).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from vedpipe.config import Settings, setup_logging
from vedpipe.spark import transforms

log = logging.getLogger(__name__)


def build_session(master: str = "local[*]", settings: Settings | None = None) -> SparkSession:
    """Local Spark session; S3A is configured only when ``settings`` are given.

    UTC session time zone and TIMESTAMP_MICROS make Parquet timestamps unambiguous
    instants that pyarrow and PostgreSQL read without conversion surprises.
    """
    builder = (
        SparkSession.builder.master(master)
        .appName("vedpipe-curate")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings is not None:
        s3a = {
            "fs.s3a.endpoint": settings.s3_endpoint_url,
            "fs.s3a.endpoint.region": settings.s3_region,
            "fs.s3a.access.key": settings.s3_access_key,
            "fs.s3a.secret.key": settings.s3_secret_key,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": str(
                settings.s3_endpoint_url.startswith("https")
            ).lower(),
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }
        for key, value in s3a.items():
            builder = builder.config(f"spark.hadoop.{key}", value)
    return builder.getOrCreate()


def read_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """All ``*.jsonl`` objects below ``raw_path``; bad lines land in ``_corrupt_record``.

    ``recursiveFileLookup`` disables partition inference, so ``ingest_date`` is parsed
    from ``source_file`` explicitly instead of being type-guessed by Spark.
    """
    return (
        spark.read.schema(transforms.raw_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl")
        .json(raw_path)
        .withColumn("source_file", F.input_file_name())
    )


def run(spark: SparkSession, raw_path: str, curated_path: str) -> list[dict[str, object]]:
    """Recompute and overwrite curated/{trips,ingest_stats,trip_points}.

    Returns the ingest statistics rows for logging and tests.
    """
    # Caching the parsed raw data is required by Spark when a query touches only the
    # corrupt-record column, and avoids re-reading the bucket for each output.
    raw = read_raw(spark, raw_path).persist(StorageLevel.MEMORY_AND_DISK)
    outputs = transforms.build_outputs(raw)
    # trips are computed from trip_points; persisting it runs the pandas kernel once.
    outputs.trip_points.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        base = curated_path.rstrip("/")
        outputs.trip_points.write.mode("overwrite").partitionBy("trip_date").parquet(
            f"{base}/trip_points"
        )
        outputs.trips.write.mode("overwrite").partitionBy("trip_date").parquet(f"{base}/trips")
        outputs.ingest_stats.coalesce(1).write.mode("overwrite").parquet(f"{base}/ingest_stats")
        stats = [row.asDict() for row in outputs.ingest_stats.orderBy("ingest_date").collect()]
    finally:
        outputs.trip_points.unpersist()
        raw.unpersist()
    for row in stats:
        log.info("ingest stats %s", row)
    return stats


def main(argv: list[str] | None = None) -> None:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default=f"s3a://{settings.raw_bucket}/trips")
    parser.add_argument("--curated", default=f"s3a://{settings.curated_bucket}")
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    args = parser.parse_args(argv)
    setup_logging()
    uses_s3 = any(p.startswith("s3a://") for p in (args.raw, args.curated))
    spark = build_session(args.master, settings if uses_s3 else None)
    try:
        run(spark, args.raw, args.curated)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
