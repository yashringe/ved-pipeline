"""Daily data-quality report per ingest date.

Each check is a rate with a warn and a fail threshold (status changes when the rate is
strictly above a threshold). The overall status is the worst check. The report is a
Markdown document stored in ``ved.dq_report`` (for the dashboard) and in the reports
bucket (for humans and audit). Evaluation and rendering are pure functions; only
:func:`main` touches PostgreSQL and S3.

``--strict`` exits 1 on FAIL so a scheduler can stop downstream publication. The compose
stack runs without it: injected faults are expected to produce WARN, and a demo should
still bring up the dashboard.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from vedpipe.config import Settings, setup_logging

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

STATUS_RANK = {"OK": 0, "WARN": 1, "FAIL": 2}
TOP_OFFENDERS = 10


@dataclass(frozen=True)
class CheckSpec:
    name: str
    label: str
    numerator: str
    denominator: str
    warn: float
    fail: float


CHECKS: tuple[CheckSpec, ...] = (
    CheckSpec("corrupt", "Corrupt records", "n_corrupt", "n_records", 0.0, 0.01),
    CheckSpec("missing_gps", "Missing GPS (no fix, or interior gap > 60 s)",
              "n_missing_gps", "n_trips", 0.05, 0.20),
    CheckSpec("gps_jump", "GPS jump or fix outside area", "n_gps_jump", "n_trips", 0.01, 0.05),
    CheckSpec("impossible_speed", "Impossible speed (out of range or spike)",
              "n_impossible_speed", "n_trips", 0.005, 0.02),
    CheckSpec("late", "Late delivery (> 24 h after trip end)", "n_late", "n_trips", 0.02, 0.10),
    CheckSpec("duplicate", "Trips delivered more than once (exact resend)",
              "n_duplicate", "n_trips", 0.02, 0.10),
)


@dataclass(frozen=True)
class Offender:
    vehicle_id: int
    vehicle_type: str
    n_trips: int
    n_flagged: int
    n_missing_gps: int
    n_gps_jump: int
    n_impossible_speed: int
    n_late: int
    n_duplicate: int


@dataclass(frozen=True)
class DayMetrics:
    """Inputs of the report for one ingest date (record counts + trip flag counts)."""

    ingest_date: date
    n_records: int = 0
    n_corrupt: int = 0
    n_exact_dup_records: int = 0
    n_superseded_records: int = 0
    n_trips: int = 0
    n_missing_gps: int = 0
    n_gps_jump: int = 0
    n_impossible_speed: int = 0
    n_late: int = 0
    n_duplicate: int = 0
    offenders: tuple[Offender, ...] = ()


@dataclass(frozen=True)
class CheckResult:
    name: str
    label: str
    count: int
    base: int
    rate: float | None
    warn: float
    fail: float
    status: str


@dataclass(frozen=True)
class Report:
    metrics: DayMetrics
    checks: tuple[CheckResult, ...]
    status: str
    generated_at: datetime
    notes: tuple[str, ...] = ()


def worst(*statuses: str) -> str:
    return max(statuses, key=STATUS_RANK.__getitem__)


def evaluate_check(spec: CheckSpec, metrics: DayMetrics) -> CheckResult:
    count = int(getattr(metrics, spec.numerator))
    base = int(getattr(metrics, spec.denominator))
    rate = count / base if base else None
    if rate is None or rate <= spec.warn:
        status = "OK"
    elif rate <= spec.fail:
        status = "WARN"
    else:
        status = "FAIL"
    return CheckResult(spec.name, spec.label, count, base, rate, spec.warn, spec.fail, status)


def build_report(metrics: DayMetrics, generated_at: datetime) -> Report:
    checks = tuple(evaluate_check(spec, metrics) for spec in CHECKS)
    status = worst("OK", *(c.status for c in checks))
    notes: list[str] = []
    if metrics.n_records == 0:
        notes.append("No records were ingested on this date.")
        status = worst(status, "WARN")
    elif metrics.n_trips == 0:
        notes.append("Records arrived but no trips were loaded for this date.")
        status = worst(status, "WARN")
    return Report(metrics, checks, status, generated_at, tuple(notes))


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2%}"


def render_markdown(report: Report) -> str:
    m = report.metrics
    lines = [
        f"# Data quality report: ingest date {m.ingest_date.isoformat()}",
        "",
        f"**Overall status: {report.status}**  ",
        f"Generated {report.generated_at.astimezone(UTC):%Y-%m-%d %H:%M} UTC",
        "",
    ]
    lines += [f"> {note}" for note in report.notes]
    if report.notes:
        lines.append("")
    lines += [
        "| Volume | Count |",
        "|---|---:|",
        f"| Records received | {m.n_records} |",
        f"| Corrupt records | {m.n_corrupt} |",
        f"| Exact duplicate records | {m.n_exact_dup_records} |",
        f"| Superseded versions | {m.n_superseded_records} |",
        f"| Trips loaded | {m.n_trips} |",
        "",
        "## Checks",
        "",
        "| Check | Count | Base | Rate | Warn above | Fail above | Status |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    lines += [
        f"| {c.label} | {c.count} | {c.base} | {_pct(c.rate)} | {_pct(c.warn)} "
        f"| {_pct(c.fail)} | {c.status} |"
        for c in report.checks
    ]
    lines += ["", f"## Top {TOP_OFFENDERS} offending vehicles", ""]
    if not m.offenders:
        lines.append("No flagged trips.")
    else:
        lines += [
            "| Vehicle | Type | Trips | Flagged | Missing GPS | GPS jump | Impossible speed "
            "| Late | Duplicate |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        lines += [
            f"| {o.vehicle_id} | {o.vehicle_type} | {o.n_trips} | {o.n_flagged} "
            f"| {o.n_missing_gps} | {o.n_gps_jump} | {o.n_impossible_speed} | {o.n_late} "
            f"| {o.n_duplicate} |"
            for o in m.offenders[:TOP_OFFENDERS]
        ]
    return "\n".join(lines) + "\n"


def report_json(report: Report) -> dict[str, Any]:
    """Machine-readable form stored next to the Markdown (jsonb column)."""
    payload = asdict(report)
    payload["metrics"]["ingest_date"] = report.metrics.ingest_date.isoformat()
    payload["generated_at"] = report.generated_at.isoformat()
    return payload


# --- I/O -----------------------------------------------------------------------------------
FLAG_COUNTS = """
    count(*) FILTER (WHERE flag_missing_gps),
    count(*) FILTER (WHERE flag_gps_jump),
    count(*) FILTER (WHERE flag_impossible_speed),
    count(*) FILTER (WHERE flag_late),
    count(*) FILTER (WHERE flag_duplicate)"""
ANY_FLAG = ("flag_missing_gps OR flag_gps_jump OR flag_impossible_speed "
            "OR flag_late OR flag_duplicate")


def latest_ingest_date(conn: psycopg.Connection) -> date | None:
    row = conn.execute("SELECT max(ingest_date) FROM ved.dq_ingest_daily").fetchone()
    return row[0] if row else None


def load_metrics(conn: psycopg.Connection, ingest_date: date) -> DayMetrics:
    records = conn.execute(
        "SELECT n_records, n_corrupt, n_exact_dup_records, n_superseded_records "
        "FROM ved.dq_ingest_daily WHERE ingest_date = %s",
        (ingest_date,),
    ).fetchone() or (0, 0, 0, 0)
    trips = conn.execute(
        f"SELECT count(*), {FLAG_COUNTS} FROM ved.fact_trip WHERE ingest_date = %s",
        (ingest_date,),
    ).fetchone()
    offenders = conn.execute(
        f"""
        SELECT vehicle_id, vehicle_type, count(*) AS n_trips,
               count(*) FILTER (WHERE {ANY_FLAG}) AS n_flagged, {FLAG_COUNTS}
        FROM ved.v_trip
        WHERE ingest_date = %s
        GROUP BY vehicle_id, vehicle_type
        HAVING count(*) FILTER (WHERE {ANY_FLAG}) > 0
        ORDER BY n_flagged DESC, vehicle_id
        LIMIT {TOP_OFFENDERS}
        """,
        (ingest_date,),
    ).fetchall()
    n_trips, *flags = trips if trips else (0, 0, 0, 0, 0, 0)
    return DayMetrics(
        ingest_date,
        *records,
        n_trips,
        *flags,
        offenders=tuple(Offender(*row) for row in offenders),
    )


def save(conn: psycopg.Connection, report: Report, markdown: str) -> None:
    conn.execute(
        """
        INSERT INTO ved.dq_report (ingest_date, status, generated_at, metrics, markdown)
        VALUES (%s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (ingest_date) DO UPDATE SET status = EXCLUDED.status,
            generated_at = EXCLUDED.generated_at, metrics = EXCLUDED.metrics,
            markdown = EXCLUDED.markdown
        """,
        (report.metrics.ingest_date, report.status, report.generated_at,
         json.dumps(report_json(report)), markdown),
    )


def main(argv: list[str] | None = None) -> int:
    import psycopg

    from vedpipe.storage import S3Storage, make_s3_client

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, help="ingest date (default: latest)")
    parser.add_argument("--strict", action="store_true", help="exit 1 when the status is FAIL")
    args = parser.parse_args(argv)
    setup_logging()
    settings = Settings.from_env()

    with psycopg.connect(settings.pg_dsn) as conn:
        ingest_date = args.date or latest_ingest_date(conn) or datetime.now(UTC).date()
        report = build_report(load_metrics(conn, ingest_date), datetime.now(UTC))
        markdown = render_markdown(report)
        save(conn, report, markdown)

    key = f"dq/ingest_date={ingest_date.isoformat()}/dq_report.md"
    S3Storage(make_s3_client(settings), settings.reports_bucket).put(
        key, markdown.encode("utf-8"), "text/markdown; charset=utf-8"
    )
    print(markdown)
    log.info("DQ status %s for %s (s3://%s/%s)", report.status, ingest_date,
             settings.reports_bucket, key)
    return 1 if args.strict and report.status == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
