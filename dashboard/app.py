"""Fleet energy and data-quality dashboard.

Reads only the ``ved`` views (agg_trip_daily, v_dq_daily), never base tables, so KPI
definitions live in SQL in one place. All ratios are recomputed here as ratios of sums
over the filtered rows, exactly like the KPI views.

Run: ``streamlit run dashboard/app.py``
"""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

PG_DSN = os.environ.get("PG_DSN", "postgresql://ved:ved@localhost:5432/ved")
# Fixed colour per powertrain (validated categorical order); UNKNOWN is neutral grey.
TYPE_ORDER = ["ICE", "HEV", "PHEV", "EV", "UNKNOWN"]
TYPE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#8a8984"]
TYPE_SCALE = alt.Scale(domain=TYPE_ORDER, range=TYPE_COLORS)
SEQUENTIAL = ["#cde2fb", "#6da7ec", "#256abf", "#0d366b"]
DIVERGING = ["#2a78d6", "#f0efec", "#e34948"]
MIN_CELL_TRIPS = 5
DQ_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
DQ_RATES = {
    "corrupt_rate": "Corrupt",
    "missing_gps_rate": "Missing GPS",
    "gps_jump_rate": "GPS jump",
    "impossible_speed_rate": "Impossible speed",
    "late_rate": "Late",
    "duplicate_rate": "Duplicate",
}


@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
    with psycopg.connect(PG_DSN) as conn:
        cursor = conn.execute(sql, params)
        columns = [col.name for col in cursor.description or []]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def energy_by(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """kWh/100 km per group as a ratio of sums (never a mean of trip ratios)."""
    grouped = frame.groupby(keys, as_index=False)[
        ["n_trips", "n_energy_trips", "energy_kwh", "energy_distance_km"]
    ].sum()
    grouped["kwh_per_100km"] = (
        100 * grouped["energy_kwh"] / grouped["energy_distance_km"].where(
            grouped["energy_distance_km"] > 0)
    )
    return grouped


def fmt(value: float | None, pattern: str) -> str:
    return "n/a" if value is None or pd.isna(value) else pattern.format(value)


st.set_page_config(page_title="VED fleet energy", layout="wide")
st.title("VED fleet energy and data quality")

# --- sidebar -------------------------------------------------------------------------------
bounds = query("SELECT min(trip_date) AS lo, max(trip_date) AS hi FROM ved.agg_trip_daily")
if bounds.empty or pd.isna(bounds.loc[0, "lo"]):
    st.info("No valid trips loaded yet. Run the pipeline (`make up` or `make pipeline`).")
    st.stop()
lo, hi = bounds.loc[0, "lo"], bounds.loc[0, "hi"]

with st.sidebar:
    st.header("Filters")
    picked = st.date_input("Trip date (local)", value=(lo, hi), min_value=lo, max_value=hi)
    start, end = (picked if isinstance(picked, tuple) and len(picked) == 2 else (lo, hi))
    all_types = query("SELECT DISTINCT vehicle_type FROM ved.agg_trip_daily")["vehicle_type"]
    present = [t for t in TYPE_ORDER if t in set(all_types)]
    types = st.multiselect("Vehicle types", present, default=present)
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    st.caption("KPIs use valid trips (>= 0.5 km, >= 60 s, no impossible speed). "
               "Energy uses trips with complete energy data for their powertrain.")

agg = query(
    "SELECT * FROM ved.agg_trip_daily WHERE trip_date BETWEEN %s AND %s",
    (start, end),
)
agg = agg[agg["vehicle_type"].isin(types)]
if agg.empty:
    st.warning("No trips match the filters.")
    st.stop()

# --- tiles ---------------------------------------------------------------------------------
n_vehicles = agg["vehicle_id"].nunique()
n_trips = int(agg["n_trips"].sum())
tiles = st.columns(6)
tiles[0].metric("Vehicles", f"{n_vehicles:,}")
tiles[1].metric("Trips", f"{n_trips:,}")
tiles[2].metric("Trips per vehicle", fmt(ratio(n_trips, n_vehicles), "{:.1f}"))
tiles[3].metric("Distance", f"{agg['distance_km'].sum():,.0f} km")
tiles[4].metric(
    "kWh/100 km",
    fmt(ratio(100 * agg["energy_kwh"].sum(), agg["energy_distance_km"].sum()), "{:.1f}"),
    help="Fleet mix of the selected types; compare types on the Energy tab.",
)
tiles[5].metric(
    "Idle share",
    fmt(ratio(agg["idle_s"].sum(), agg["idle_s"].sum() + agg["moving_s"].sum()), "{:.1%}"),
)

energy_tab, usage_tab, dq_tab = st.tabs(["Energy", "Usage", "Data quality"])

# --- energy --------------------------------------------------------------------------------
with energy_tab:
    by_type = energy_by(agg, ["vehicle_type"])
    bars = alt.Chart(by_type).encode(
        x=alt.X("vehicle_type:N", sort=TYPE_ORDER, title=None),
        y=alt.Y("kwh_per_100km:Q", title="kWh/100 km"),
        tooltip=[
            alt.Tooltip("vehicle_type:N", title="Type"),
            alt.Tooltip("kwh_per_100km:Q", title="kWh/100 km", format=".1f"),
            alt.Tooltip("n_energy_trips:Q", title="Trips with complete energy"),
        ],
    )
    st.subheader("Energy intensity by powertrain")
    st.altair_chart(
        bars.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=48).encode(
            color=alt.Color("vehicle_type:N", scale=TYPE_SCALE, legend=None)
        )
        + bars.mark_text(dy=-8).encode(text=alt.Text("kwh_per_100km:Q", format=".1f")),
        use_container_width=True,
    )

    st.subheader("Energy intensity by start hour")
    mode = st.radio(
        "Show", ["Absolute (kWh/100 km)", "Index vs type average (100 = type mean)"],
        horizontal=True,
    )
    cells = energy_by(agg, ["vehicle_type", "start_hour_local"])
    cells = cells[cells["n_energy_trips"] >= MIN_CELL_TRIPS]
    type_mean = by_type.set_index("vehicle_type")["kwh_per_100km"]
    cells["index"] = 100 * cells["kwh_per_100km"] / cells["vehicle_type"].map(type_mean)
    if mode.startswith("Absolute"):
        value, title, scale = "kwh_per_100km", "kWh/100 km", alt.Scale(range=SEQUENTIAL)
    else:
        value, title, scale = "index", "Index", alt.Scale(range=DIVERGING, domainMid=100)
    st.altair_chart(
        alt.Chart(cells)
        .mark_rect(cornerRadius=2)
        .encode(
            x=alt.X("start_hour_local:O", title="Start hour (America/Detroit)"),
            y=alt.Y("vehicle_type:N", sort=TYPE_ORDER, title=None),
            color=alt.Color(f"{value}:Q", title=title, scale=scale),
            tooltip=[
                alt.Tooltip("vehicle_type:N", title="Type"),
                alt.Tooltip("start_hour_local:O", title="Hour"),
                alt.Tooltip("kwh_per_100km:Q", title="kWh/100 km", format=".1f"),
                alt.Tooltip("index:Q", title="Index", format=".0f"),
                alt.Tooltip("n_energy_trips:Q", title="Trips"),
            ],
        )
        .properties(height=220),
        use_container_width=True,
    )
    st.caption(f"Cells with fewer than {MIN_CELL_TRIPS} energy-complete trips are hidden.")

    st.subheader("Trips by start hour")
    hourly = agg.groupby(["start_hour_local", "vehicle_type"], as_index=False)["n_trips"].sum()
    st.altair_chart(
        alt.Chart(hourly)
        .mark_bar()
        .encode(
            x=alt.X("start_hour_local:O", title="Start hour (America/Detroit)"),
            y=alt.Y("n_trips:Q", title="Trips", stack=True),
            color=alt.Color("vehicle_type:N", scale=TYPE_SCALE, title="Type"),
            order=alt.Order("vehicle_type:N"),
            tooltip=["start_hour_local:O", "vehicle_type:N", "n_trips:Q"],
        ),
        use_container_width=True,
    )

# --- usage ---------------------------------------------------------------------------------
with usage_tab:
    left, right = st.columns(2)
    per_vehicle = agg.groupby("vehicle_id", as_index=False)["n_trips"].sum()
    left.subheader("Trips per vehicle")
    left.altair_chart(
        alt.Chart(per_vehicle)
        .mark_bar(color=TYPE_COLORS[0])
        .encode(
            x=alt.X("n_trips:Q", bin=alt.Bin(maxbins=30), title="Trips in period"),
            y=alt.Y("count():Q", title="Vehicles"),
            tooltip=[alt.Tooltip("count():Q", title="Vehicles")],
        ),
        use_container_width=True,
    )
    idle = agg.groupby("vehicle_type", as_index=False)[["idle_s", "moving_s"]].sum()
    idle["idle_share"] = idle["idle_s"] / (idle["idle_s"] + idle["moving_s"])
    right.subheader("Idle share by powertrain")
    idle_bars = alt.Chart(idle).encode(
        x=alt.X("vehicle_type:N", sort=TYPE_ORDER, title=None),
        y=alt.Y("idle_share:Q", title="Idle share", axis=alt.Axis(format="%")),
        tooltip=[alt.Tooltip("vehicle_type:N", title="Type"),
                 alt.Tooltip("idle_share:Q", title="Idle share", format=".1%")],
    )
    right.altair_chart(
        idle_bars.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=48).encode(
            color=alt.Color("vehicle_type:N", scale=TYPE_SCALE, legend=None)
        )
        + idle_bars.mark_text(dy=-8).encode(text=alt.Text("idle_share:Q", format=".1%")),
        use_container_width=True,
    )

# --- data quality --------------------------------------------------------------------------
with dq_tab:
    dq = query("SELECT * FROM ved.v_dq_daily ORDER BY ingest_date")
    st.caption("Data quality is tracked per ingest date (when data arrived), not trip date.")
    reports = dq.dropna(subset=["markdown"])
    if reports.empty:
        st.info("No DQ report yet. It is produced by the dq-report step.")
    else:
        latest = reports.iloc[-1]
        st.markdown(latest["markdown"])
    if not dq.empty:
        history = dq.melt(
            id_vars=["ingest_date"], value_vars=list(DQ_RATES), var_name="check",
            value_name="rate",
        ).dropna()
        history["check"] = history["check"].map(DQ_RATES)
        st.subheader("History")
        st.altair_chart(
            alt.Chart(history)
            .mark_line(point=alt.OverlayMarkDef(size=64), strokeWidth=2)
            .encode(
                x=alt.X("ingest_date:T", title="Ingest date"),
                y=alt.Y("rate:Q", title="Rate", axis=alt.Axis(format="%")),
                color=alt.Color(
                    "check:N", title="Check",
                    scale=alt.Scale(domain=list(DQ_RATES.values()), range=DQ_COLORS),
                ),
                tooltip=["ingest_date:T", "check:N", alt.Tooltip("rate:Q", format=".2%")],
            ),
            use_container_width=True,
        )
        st.dataframe(
            dq.drop(columns=["markdown"]).sort_values("ingest_date", ascending=False),
            use_container_width=True, hide_index=True,
        )
