-- Semantic layer: every consumer (dashboard, `make kpis`, ad-hoc SQL) reads these views.
-- Views are dropped and recreated on every load so definition changes always apply
-- (CREATE OR REPLACE VIEW cannot drop or reorder columns).
--
-- KPI rules:
--   * KPIs use valid trips only: >= 0.5 km, >= 60 s, no impossible speed.
--   * kWh/100 km = 100 * SUM(energy_kwh) / SUM(distance_km) over the SAME trips, namely
--     valid trips with complete energy data for their powertrain. Never an average of
--     per-trip ratios, which would over-weight short trips.
--   * Idle share = SUM(idle_s) / SUM(idle_s + moving_s).

DROP VIEW IF EXISTS
    ved.v_dq_daily,
    ved.kpi_fleet,
    ved.kpi_vehicle,
    ved.kpi_energy_by_type_hour,
    ved.kpi_energy_by_type,
    ved.agg_trip_daily,
    ved.v_trip
CASCADE;

-- Trip grain with vehicle attributes and KPI eligibility.
CREATE VIEW ved.v_trip AS
SELECT
    t.*,
    COALESCE(v.vehicle_type, 'UNKNOWN') AS vehicle_type,
    v.vehicle_class,
    EXTRACT(HOUR FROM t.started_at AT TIME ZONE 'America/Detroit')::int AS start_hour_local,
    (t.distance_km >= 0.5 AND t.duration_s >= 60 AND NOT t.flag_impossible_speed)
        AS is_valid_trip,
    -- Energy is complete when the powertrain's energy sources are observed: fuel for
    -- ICE/HEV (HEVs report no battery signals), battery for EV, both for PHEV.
    CASE COALESCE(v.vehicle_type, 'UNKNOWN')
        WHEN 'ICE'  THEN t.has_fuel_data
        WHEN 'HEV'  THEN t.has_fuel_data
        WHEN 'EV'   THEN t.has_batt_data
        WHEN 'PHEV' THEN t.has_fuel_data AND t.has_batt_data
        ELSE false
    END AS has_complete_energy
FROM ved.fact_trip AS t
LEFT JOIN ved.dim_vehicle AS v ON v.vehicle_id = t.vehicle_id;

-- Additive daily aggregate of valid trips. Ratios are computed only after summing.
CREATE VIEW ved.agg_trip_daily AS
SELECT
    trip_date,
    vehicle_id,
    vehicle_type,
    start_hour_local,
    count(*)                                                          AS n_trips,
    sum(distance_km)                                                  AS distance_km,
    sum(duration_s)                                                   AS duration_s,
    sum(moving_s)                                                     AS moving_s,
    sum(idle_s)                                                       AS idle_s,
    sum(fuel_l)                                                       AS fuel_l,
    sum(elec_kwh)                                                     AS elec_kwh,
    count(*) FILTER (WHERE has_complete_energy)                       AS n_energy_trips,
    coalesce(sum(energy_kwh) FILTER (WHERE has_complete_energy), 0)  AS energy_kwh,
    coalesce(sum(distance_km) FILTER (WHERE has_complete_energy), 0) AS energy_distance_km
FROM ved.v_trip
WHERE is_valid_trip
GROUP BY trip_date, vehicle_id, vehicle_type, start_hour_local;

CREATE VIEW ved.kpi_energy_by_type AS
SELECT
    vehicle_type,
    count(DISTINCT vehicle_id)                                   AS n_vehicles,
    sum(n_trips)                                                 AS n_trips,
    sum(n_energy_trips)                                          AS n_energy_trips,
    sum(energy_distance_km)                                      AS energy_distance_km,
    sum(energy_kwh)                                              AS energy_kwh,
    100 * sum(energy_kwh) / NULLIF(sum(energy_distance_km), 0)   AS kwh_per_100km
FROM ved.agg_trip_daily
GROUP BY vehicle_type;

CREATE VIEW ved.kpi_energy_by_type_hour AS
SELECT
    vehicle_type,
    start_hour_local,
    sum(n_trips)                                                 AS n_trips,
    sum(n_energy_trips)                                          AS n_energy_trips,
    100 * sum(energy_kwh) / NULLIF(sum(energy_distance_km), 0)   AS kwh_per_100km
FROM ved.agg_trip_daily
GROUP BY vehicle_type, start_hour_local;

-- Per vehicle: usage and energy over valid trips, data quality over all trips.
CREATE VIEW ved.kpi_vehicle AS
SELECT
    vehicle_id,
    vehicle_type,
    count(*)                                                        AS n_trips_total,
    count(*) FILTER (WHERE is_valid_trip)                           AS n_trips,
    coalesce(sum(distance_km) FILTER (WHERE is_valid_trip), 0)      AS distance_km,
    100 * sum(energy_kwh) FILTER (WHERE is_valid_trip AND has_complete_energy)
        / NULLIF(sum(distance_km) FILTER (WHERE is_valid_trip AND has_complete_energy), 0)
                                                                    AS kwh_per_100km,
    sum(idle_s) FILTER (WHERE is_valid_trip)
        / NULLIF(sum(idle_s + moving_s) FILTER (WHERE is_valid_trip), 0) AS idle_share,
    count(*) FILTER (WHERE flag_missing_gps OR flag_gps_jump OR flag_impossible_speed
                        OR flag_late OR flag_duplicate)             AS n_flagged_trips,
    min(trip_date)                                                  AS first_trip_date,
    max(trip_date)                                                  AS last_trip_date
FROM ved.v_trip
GROUP BY vehicle_id, vehicle_type;

-- Fleet headline numbers per powertrain plus an 'ALL' row (ROLLUP).
CREATE VIEW ved.kpi_fleet AS
SELECT
    CASE WHEN GROUPING(vehicle_type) = 1 THEN 'ALL' ELSE vehicle_type END AS vehicle_type,
    count(DISTINCT vehicle_id)                                            AS n_vehicles,
    sum(n_trips)                                                          AS n_trips,
    sum(n_trips)::double precision / NULLIF(count(DISTINCT vehicle_id), 0)
                                                                          AS trips_per_vehicle,
    sum(distance_km)                                                      AS distance_km,
    100 * sum(energy_kwh) / NULLIF(sum(energy_distance_km), 0)            AS kwh_per_100km,
    sum(idle_s) / NULLIF(sum(idle_s) + sum(moving_s), 0)                  AS idle_share,
    min(trip_date)                                                        AS first_trip_date,
    max(trip_date)                                                        AS last_trip_date
FROM ved.agg_trip_daily
GROUP BY ROLLUP (vehicle_type);

-- Data quality per ingest day: job counts, flag rates of the trips ingested that day,
-- and the rendered report.
CREATE VIEW ved.v_dq_daily AS
WITH trip_flags AS (
    SELECT
        ingest_date,
        count(*)                                           AS n_trips_loaded,
        avg(flag_missing_gps::int)::double precision       AS missing_gps_rate,
        avg(flag_gps_jump::int)::double precision          AS gps_jump_rate,
        avg(flag_impossible_speed::int)::double precision  AS impossible_speed_rate,
        avg(flag_late::int)::double precision              AS late_rate,
        avg(flag_duplicate::int)::double precision         AS duplicate_rate
    FROM ved.fact_trip
    GROUP BY ingest_date
)
SELECT
    s.ingest_date,
    s.n_records,
    s.n_corrupt,
    s.n_trips,
    s.n_exact_dup_records,
    s.n_superseded_records,
    s.n_corrupt::double precision / NULLIF(s.n_records, 0) AS corrupt_rate,
    f.n_trips_loaded,
    f.missing_gps_rate,
    f.gps_jump_rate,
    f.impossible_speed_rate,
    f.late_rate,
    f.duplicate_rate,
    r.status,
    r.generated_at,
    r.markdown
FROM ved.dq_ingest_daily AS s
LEFT JOIN trip_flags AS f ON f.ingest_date = s.ingest_date
LEFT JOIN ved.dq_report AS r ON r.ingest_date = s.ingest_date;
