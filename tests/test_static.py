"""VED static data normalisation and the cross-layer schema contract."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from vedpipe.load.loader import (
    Vehicle,
    map_vehicle_type,
    normalise_static,
    read_static,
    upsert_trips_sql,
)
from vedpipe.schema import INGEST_STATS_COLUMNS, SIGNAL_FIELDS, TRIP_COLUMNS, column_names

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def static_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            " VehId ": [8, 10, "455", None],
            "EngineType": ["ICE", "HEV", "EV", "ICE"],
            "Vehicle Class": ["Car", "NO DATA", "Car", "SUV"],
            "Engine Configuration & Displacement": ["I4 2.0L", "I4 1.8L", "NO DATA", "V6"],
            "Transmission": ["5-SP Automatic", "CVT", "NO DATA", "6-SP"],
            "Drive Wheels": ["FWD", "FWD", "FWD", "AWD"],
            "Generalized_Weight[lb]": [3500, "3,000", "NO DATA", 4000],
        }
    )


def test_normalise_static_maps_headers_types_and_no_data() -> None:
    vehicles = normalise_static(static_frame(), "VED_Static_Data_ICE&HEV.xlsx")

    assert vehicles[0] == Vehicle(8, "ICE", "ICE", "Car", "I4 2.0L", "5-SP Automatic", "FWD",
                                  3500, "VED_Static_Data_ICE&HEV.xlsx")
    assert vehicles[1].vehicle_type == "HEV"
    assert vehicles[1].vehicle_class is None  # "NO DATA" -> NULL
    assert vehicles[1].weight_lb == 3000
    assert vehicles[2].vehicle_id == 455
    assert vehicles[2].vehicle_type == "EV"
    assert vehicles[2].engine_config is None
    assert vehicles[2].weight_lb is None
    assert len(vehicles) == 3  # row without VehId skipped


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("ICE", "ICE"), ("HEV", "HEV"), ("PHEV", "PHEV"), ("EV", "EV"), ("BEV", "EV"),
     ("Plug-in Hybrid", "PHEV"), ("hybrid", "HEV"), (None, "UNKNOWN"), ("NO DATA", "UNKNOWN")],
)
def test_vehicle_type_mapping(raw: str | None, expected: str) -> None:
    assert map_vehicle_type(raw) == expected


def test_static_without_vehid_column_fails_loudly() -> None:
    with pytest.raises(ValueError, match="VehId"):
        normalise_static(pd.DataFrame({"EngineType": ["ICE"]}), "bad.xlsx")


def test_read_static_from_xlsx_files(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    static_frame().to_excel(tmp_path / "VED_Static_Data_ICE&HEV.xlsx", index=False)
    pd.DataFrame(
        {"VehId": [541, 8], "EngineType": ["PHEV", "ICE"], "Generalized_Weight": [4500, 3600]}
    ).to_excel(tmp_path / "VED_Static_Data_PHEV&EV.xlsx", index=False)

    vehicles = {v.vehicle_id: v for v in read_static(tmp_path)}

    assert set(vehicles) == {8, 10, 455, 541}
    assert vehicles[541].vehicle_type == "PHEV"
    assert vehicles[8].weight_lb == 3600  # later file wins on a duplicate VehId
    assert vehicles[8].source_file == "VED_Static_Data_PHEV&EV.xlsx"


def table_columns(sql: str, table: str) -> list[str]:
    body = re.search(rf"CREATE TABLE IF NOT EXISTS ved\.{table} \((.*?)\n\);", sql, re.S)
    assert body, table
    names = [line.split()[0] for line in body.group(1).splitlines() if line.strip()]
    return [n for n in names if n.islower() and not n.startswith("--")]


def test_sql_tables_match_schema_contract() -> None:
    sql = (SQL_DIR / "001_schema.sql").read_text()
    assert table_columns(sql, "fact_trip") == [*column_names(TRIP_COLUMNS), "loaded_at"]
    assert table_columns(sql, "dq_ingest_daily") == [
        *column_names(INGEST_STATS_COLUMNS), "loaded_at",
    ]


def test_upsert_updates_every_non_key_column() -> None:
    sql = upsert_trips_sql()
    for name in column_names(TRIP_COLUMNS)[2:]:
        assert f"{name} = EXCLUDED.{name}" in sql
    assert "vehicle_id = EXCLUDED" not in sql


def test_api_point_model_matches_signal_fields() -> None:
    pytest.importorskip("pydantic")
    from vedpipe.api.models import TripPoint

    assert list(TripPoint.model_fields) == ["t", *SIGNAL_FIELDS]
