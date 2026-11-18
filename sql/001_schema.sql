-- Warehouse tables. Applied by the loader on every run: every statement is idempotent.
-- Column order of fact_trip matches vedpipe.schema.TRIP_COLUMNS (checked by a test).

CREATE SCHEMA IF NOT EXISTS ved;

-- Vehicles from the VED static xlsx files. vehicle_type is normalised to ICE/HEV/PHEV/EV.
CREATE TABLE IF NOT EXISTS ved.dim_vehicle (
    vehicle_id       bigint PRIMARY KEY,
    vehicle_type     text        NOT NULL
        CHECK (vehicle_type IN ('ICE', 'HEV', 'PHEV', 'EV', 'UNKNOWN')),
    engine_type_raw  text,
    vehicle_class    text,
    engine_config    text,
    transmission     text,
    drive_wheels     text,
    weight_lb        integer,
    source_file      text        NOT NULL,
    loaded_at        timestamptz NOT NULL DEFAULT now()
);

-- One row per trip. No FK to dim_vehicle on purpose: a partner may deliver trips for a
-- vehicle before its static data exists, and those trips must still load (they show up
-- as vehicle_type 'UNKNOWN' in the views).
CREATE TABLE IF NOT EXISTS ved.fact_trip (
    vehicle_id             bigint           NOT NULL,
    trip_id                bigint           NOT NULL,
    partner_id             text,
    started_at             timestamptz      NOT NULL,
    ended_at               timestamptz      NOT NULL,
    trip_date              date             NOT NULL,
    ingest_date            date,
    duration_s             double precision NOT NULL,
    distance_km            double precision NOT NULL,
    moving_s               double precision NOT NULL,
    idle_s                 double precision NOT NULL,
    gap_s                  double precision NOT NULL,
    max_speed_kmh          double precision,
    fuel_l                 double precision NOT NULL,
    fuel_kwh               double precision NOT NULL,
    elec_kwh               double precision NOT NULL,
    energy_kwh             double precision NOT NULL,
    fuel_coverage          double precision NOT NULL,
    batt_coverage          double precision NOT NULL,
    has_fuel_data          boolean          NOT NULL,
    has_batt_data          boolean          NOT NULL,
    n_points               bigint           NOT NULL,
    n_dup_points           bigint           NOT NULL,
    n_speed_out_of_range   bigint           NOT NULL,
    n_accel_spikes         bigint           NOT NULL,
    n_gps_fixes            bigint           NOT NULL,
    n_gps_out_of_area      bigint           NOT NULL,
    n_gps_jumps            bigint           NOT NULL,
    max_gps_gap_s          double precision,
    n_records              bigint           NOT NULL,
    n_versions             bigint           NOT NULL,
    n_exact_dups           bigint           NOT NULL,
    sent_at                timestamptz,
    received_at            timestamptz      NOT NULL,
    delay_s                double precision NOT NULL,
    flag_missing_gps       boolean          NOT NULL,
    flag_gps_jump          boolean          NOT NULL,
    flag_impossible_speed  boolean          NOT NULL,
    flag_late              boolean          NOT NULL,
    flag_duplicate         boolean          NOT NULL,
    loaded_at              timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, trip_id)
);

CREATE INDEX IF NOT EXISTS fact_trip_trip_date_idx ON ved.fact_trip (trip_date);
CREATE INDEX IF NOT EXISTS fact_trip_ingest_date_idx ON ved.fact_trip (ingest_date);

-- Staging for COPY. Unlogged (no WAL: it is rebuilt from Parquet on every run) and
-- recreated each run so it always mirrors fact_trip's current columns.
DROP TABLE IF EXISTS ved.stg_trip;
CREATE UNLOGGED TABLE ved.stg_trip (LIKE ved.fact_trip INCLUDING DEFAULTS);

-- Per ingest day counts from the curation job (replaced on every load).
CREATE TABLE IF NOT EXISTS ved.dq_ingest_daily (
    ingest_date           date PRIMARY KEY,
    n_records             bigint      NOT NULL,
    n_corrupt             bigint      NOT NULL,
    n_trips               bigint      NOT NULL,
    n_exact_dup_records   bigint      NOT NULL,
    n_superseded_records  bigint      NOT NULL,
    loaded_at             timestamptz NOT NULL DEFAULT now()
);

-- Rendered daily data-quality reports (one per ingest day, latest run wins).
CREATE TABLE IF NOT EXISTS ved.dq_report (
    ingest_date   date PRIMARY KEY,
    status        text        NOT NULL CHECK (status IN ('OK', 'WARN', 'FAIL')),
    generated_at  timestamptz NOT NULL,
    metrics       jsonb       NOT NULL,
    markdown      text        NOT NULL
);
