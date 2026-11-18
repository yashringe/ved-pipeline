"""DQ report: thresholds, overall status, empty days and the offender table."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from vedpipe.quality.report import (
    DayMetrics,
    Offender,
    build_report,
    render_markdown,
    report_json,
)

DAY = date(2026, 11, 17)
GENERATED = datetime(2026, 11, 17, 20, 0, tzinfo=UTC)


def metrics(**overrides: object) -> DayMetrics:
    values: dict[str, object] = {"n_records": 1000, "n_trips": 1000}
    values.update(overrides)
    return DayMetrics(DAY, **values)  # type: ignore[arg-type]


def status_of(report: object, name: str) -> str:
    return next(c.status for c in report.checks if c.name == name)  # type: ignore[attr-defined]


def test_clean_day_is_ok() -> None:
    report = build_report(metrics(), GENERATED)
    assert report.status == "OK"
    assert all(c.status == "OK" for c in report.checks)


def test_rate_between_thresholds_warns() -> None:
    report = build_report(metrics(n_late=30), GENERATED)  # 3 % > 2 % warn, <= 10 % fail
    assert status_of(report, "late") == "WARN"
    assert report.status == "WARN"


def test_rate_above_fail_threshold_fails() -> None:
    report = build_report(metrics(n_impossible_speed=25, n_late=30), GENERATED)  # 2.5 % > 2 %
    assert status_of(report, "impossible_speed") == "FAIL"
    assert report.status == "FAIL"


def test_threshold_is_exclusive() -> None:
    report = build_report(metrics(n_missing_gps=50), GENERATED)  # exactly 5 %
    assert status_of(report, "missing_gps") == "OK"


@pytest.mark.parametrize(("corrupt", "expected"), [(0, "OK"), (1, "WARN"), (11, "FAIL")])
def test_any_corrupt_record_warns(corrupt: int, expected: str) -> None:
    report = build_report(metrics(n_corrupt=corrupt), GENERATED)
    assert status_of(report, "corrupt") == expected


def test_empty_day_warns_and_says_so() -> None:
    report = build_report(DayMetrics(DAY), GENERATED)
    markdown = render_markdown(report)

    assert report.status == "WARN"
    assert all(c.rate is None for c in report.checks)
    assert "No records were ingested" in markdown
    assert "No flagged trips." in markdown
    assert "| n/a |" in markdown


def test_offender_table_is_capped_at_ten() -> None:
    offenders = tuple(
        Offender(vehicle_id=v, vehicle_type="ICE", n_trips=20, n_flagged=20 - v,
                 n_missing_gps=1, n_gps_jump=0, n_impossible_speed=0, n_late=1, n_duplicate=0)
        for v in range(1, 13)
    )
    markdown = render_markdown(build_report(metrics(offenders=offenders), GENERATED))

    rows = [line for line in markdown.splitlines() if line.startswith("| ") and "| ICE |" in line]
    assert len(rows) == 10
    assert rows[0].startswith("| 1 | ICE | 20 | 19 |")
    assert "## Top 10 offending vehicles" in markdown


def test_markdown_contains_volumes_and_check_rows() -> None:
    markdown = render_markdown(build_report(metrics(n_corrupt=1, n_late=30), GENERATED))
    assert markdown.startswith("# Data quality report: ingest date 2026-11-17")
    assert "**Overall status: WARN**" in markdown
    assert "| Late delivery (> 24 h after trip end) | 30 | 1000 | 3.00% | 2.00% | 10.00% | WARN |" \
        in markdown
    assert "| Records received | 1000 |" in markdown


def test_report_json_is_serialisable() -> None:
    payload = report_json(build_report(metrics(n_late=30), GENERATED))
    decoded = json.loads(json.dumps(payload))
    assert decoded["status"] == "WARN"
    assert decoded["metrics"]["ingest_date"] == "2026-11-17"
