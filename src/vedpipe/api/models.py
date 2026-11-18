"""Trip data contract v1.0.

The API validates *structure only*: types, required fields, timezone-aware timestamps,
non-negative offsets and finite numbers. Physically implausible values (a 320 km/h
sample, a fix in the ocean) are accepted on purpose: rejecting them here would lose
the evidence, while the curation job can flag and measure them per partner.
Unknown fields are kept (``extra="allow"``) so partners can roll out new signals
before the contract catches up.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

CONTRACT_VERSION = "1.0"
MAX_BATCH_TRIPS = 500


def _assert_finite(value: Any, path: str) -> None:
    """Reject NaN/inf anywhere in free-form extras: they are not valid JSON numbers."""
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} must be a finite number")
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_finite(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_finite(item, f"{path}[{index}]")


class _OpenModel(BaseModel):
    """Keeps unknown fields but still refuses non-finite numbers in them."""

    model_config = ConfigDict(extra="allow", allow_inf_nan=False)

    @model_validator(mode="after")
    def _extras_are_finite(self) -> Self:
        for key, value in (self.model_extra or {}).items():
            _assert_finite(value, key)
        return self


class TripPoint(_OpenModel):
    """One sample. Signals that were not measured at ``t`` are omitted, never zero-filled."""

    t: int = Field(ge=0, description="Milliseconds since the trip's started_at.")
    lat: float | None = Field(None, description="Latitude, degrees (WGS84).")
    lon: float | None = Field(None, description="Longitude, degrees (WGS84).")
    speed_kmh: float | None = Field(None, description="OBD vehicle speed, km/h.")
    maf_g_s: float | None = Field(None, description="Mass air flow, g/s.")
    engine_rpm: float | None = Field(None, description="Engine speed, rev/min.")
    abs_load_pct: float | None = Field(None, description="Absolute engine load, %.")
    oat_c: float | None = Field(None, description="Outside air temperature, deg C.")
    fuel_rate_l_h: float | None = Field(None, description="Fuel rate, L/h.")
    ac_power_kw: float | None = Field(None, description="Air-conditioning power, kW.")
    ac_power_w: float | None = Field(None, description="Air-conditioning power, W.")
    heater_power_w: float | None = Field(None, description="Heater power, W.")
    hv_current_a: float | None = Field(
        None, description="HV battery current, A (positive = charging, negative = discharging)."
    )
    hv_soc_pct: float | None = Field(None, description="HV battery state of charge, %.")
    hv_voltage_v: float | None = Field(None, description="HV battery voltage, V.")
    stft_b1_pct: float | None = Field(None, description="Short-term fuel trim bank 1, %.")
    stft_b2_pct: float | None = Field(None, description="Short-term fuel trim bank 2, %.")
    ltft_b1_pct: float | None = Field(None, description="Long-term fuel trim bank 1, %.")
    ltft_b2_pct: float | None = Field(None, description="Long-term fuel trim bank 2, %.")


class TripPayload(_OpenModel):
    """A complete (or partial, then re-sent) trip as delivered by a partner."""

    vehicle_id: int
    trip_id: int = Field(gt=0)
    started_at: AwareDatetime = Field(description="Trip start; must carry a UTC offset.")
    sent_at: AwareDatetime | None = Field(
        None, description="Partner clock at send time; used for lateness and versioning."
    )
    points: list[TripPoint] = Field(min_length=1)


class TripBatch(BaseModel):
    """Up to 500 trips persisted as one raw object."""

    model_config = ConfigDict(extra="forbid")

    trips: list[TripPayload] = Field(min_length=1, max_length=MAX_BATCH_TRIPS)


class AcceptedRecord(BaseModel):
    record_id: str
    vehicle_id: int
    trip_id: int
    content_sha256: str


class IngestResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    object_key: str
    received_at: datetime
    records: list[AcceptedRecord]
