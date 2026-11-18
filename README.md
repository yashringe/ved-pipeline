# ved-pipeline

Partner trip ingestion, curation and fleet-energy KPIs on the
[Vehicle Energy Dataset (VED)](https://github.com/gsoh/VED): 383 personal cars (264 ICE,
92 HEV, 24 PHEV, 3 EV) driven in Ann Arbor, Michigan, from Nov 2017 to Nov 2018.

A simulated partner replays VED trips to an ingest API, with realistic delivery faults.
Raw NDJSON lands in object storage, a PySpark job recomputes clean trip facts, a loader
publishes them to PostgreSQL, a daily data-quality report grades each ingest day, and a
Streamlit dashboard shows energy intensity and usage per powertrain.

## Architecture

```mermaid
flowchart LR
    VED[(VED weekly CSVs<br/>+ static xlsx)] --> SIM[partner-sim<br/>replay + faults]
    SIM -->|POST /v1/trips/batch<br/>X-API-Key| API[FastAPI ingest]
    API -->|1 NDJSON object<br/>per request| RAW[(MinIO raw<br/>trips/ingest_date=/partner=)]
    RAW --> SPARK[PySpark curation<br/>dedupe, rules, integrals]
    SPARK --> CUR[(MinIO curated<br/>Parquet)]
    CUR --> LOAD[loader<br/>one transaction]
    VED -. static data .-> LOAD
    LOAD --> PG[(PostgreSQL<br/>tables + KPI views)]
    PG --> DQ[daily DQ report]
    DQ --> PG
    DQ --> REP[(MinIO reports<br/>dq_report.md)]
    PG --> DASH[Streamlit dashboard]
```

| Step | Module | Output |
|---|---|---|
| fetch-ved | `vedpipe.sim.fetch` | static xlsx + first `VED_WEEKS` weekly CSVs in a volume |
| partner-sim | `vedpipe.sim.replay` | trips sent in arrival order; `injected_faults.json` ground truth |
| api | `vedpipe.api.main` | `raw/trips/ingest_date=YYYY-MM-DD/partner=<p>/<time>-<id>.jsonl` |
| spark-job | `vedpipe.spark.job` | `curated/trips`, `curated/ingest_stats`, `curated/trip_points` |
| loader | `vedpipe.load.loader` | `ved.dim_vehicle`, `ved.fact_trip`, `ved.dq_ingest_daily`, views |
| dq-report | `vedpipe.quality.report` | `ved.dq_report` + `reports/dq/ingest_date=.../dq_report.md` |
| dashboard | `dashboard/app.py` | http://localhost:8501 |

## Quick start

Requirements: Docker with Compose v2, ~6 GB free disk, internet access (GitHub, Docker
Hub, quay.io, Maven Central, PyPI).

```bash
cp .env.example .env   # optional: change VED_WEEKS, fault rates, credentials
make up                # builds images and runs the whole chain (first run: 10-20 min)
make kpis              # headline KPIs in the terminal
```

`make up` returns once the one-shot steps have finished; the API, MinIO, PostgreSQL and
the dashboard keep running. Re-running `make up` does not re-download or re-send data
(the fetch manifest and the `.replayed` marker make both steps no-ops). To reprocess the
current raw data after a code change: `make pipeline`.

### Service URLs

| Service | URL | Credentials (local defaults) |
|---|---|---|
| Dashboard | http://localhost:8501 | none |
| Ingest API docs | http://localhost:8000/docs | header `X-API-Key: dev-key-a` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| PostgreSQL | `postgresql://ved:ved@localhost:5432/ved` | `ved` / `ved` |

The MinIO image (`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`) is a frozen,
unpatched community build: fine for a laptop demo, never for anything reachable from a
network. Buckets are created from Python with boto3; the stack does not depend on the
`mc` client image.

## Data contract v1.0

`POST /v1/trips` (one trip) or `POST /v1/trips/batch` (`{"trips": [...]}`, max 500), header
`X-API-Key`. The partner is derived from the key, never from the body.

```json
{
  "vehicle_id": 8,
  "trip_id": 706,
  "started_at": "2017-11-01T12:00:00.000-04:00",
  "sent_at": "2017-11-01T12:31:10.000-04:00",
  "points": [
    {"t": 0, "speed_kmh": 0.0, "lat": 42.2776, "lon": -83.6982},
    {"t": 1000, "speed_kmh": 3.4, "maf_g_s": 2.1},
    {"t": 2000, "speed_kmh": 6.9, "hv_current_a": -12.5, "hv_voltage_v": 362.1}
  ]
}
```

* `t` is milliseconds since `started_at` (integer, >= 0); timestamps must carry an offset.
* Signals not measured at `t` are omitted. Units are in the names (see
  `vedpipe.schema.SIGNAL_FIELDS`). `hv_current_a` is positive when charging.
* Only structure is validated (422). Implausible values are accepted and flagged
  downstream. Unknown fields are kept. NaN/Infinity are rejected.
* Response `202` with one `record_id` and `content_sha256` per trip. `503` (with
  `Retry-After`) means storage was unavailable: retry. The hash ignores `sent_at`, so a
  retried send is recognised as an exact duplicate.

Raw envelope (one line per trip, one object per request):
`{"record_id", "received_at", "partner_id", "content_sha256", "payload"}`.

## Data-quality rules

| Rule | Where | Effect | Trip flag | Daily check warn / fail |
|---|---|---|---|---|
| Line not parseable, or no key / start / points | Spark | record dropped, counted | n/a | corrupt records > 0 % / > 1 % |
| Same trip, same content hash | Spark | first copy kept; lateness measured on it | `flag_duplicate` | > 2 % / > 10 % of trips |
| Several versions of a trip | Spark | newest partner `sent_at` wins | n/a (counted) | n/a |
| Duplicate `t` within a trip | Spark | first sample kept, counted | n/a | n/a |
| Speed < 0 or > 200 km/h | kernel | value nulled | `flag_impossible_speed` | > 0.5 % / > 2 % |
| abs(accel) > 15 m/s² between valid samples 0.5-5 s apart | kernel | counted | `flag_impossible_speed` | (same) |
| Fix outside lat 41.9-42.6, lon -84.2 to -83.3 | kernel | fix nulled | `flag_gps_jump` | > 1 % / > 5 % |
| Consecutive fixes > 0.1 km apart implying > 250 km/h | kernel | counted | `flag_gps_jump` | (same) |
| No fix, or an interior gap between fixes > 60 s | kernel | measured | `flag_missing_gps` | > 5 % / > 20 % |
| Delivered > 24 h after trip end (`sent_at`, else `received_at`) | Spark | measured | `flag_late` | > 2 % / > 10 % |

Thresholds are exclusive (a rate equal to the warn threshold is OK). The overall status is
the worst check; a day without records is WARN. `python -m vedpipe.quality.report --strict`
exits 1 on FAIL.

## KPI definitions

* **Valid trip:** distance >= 0.5 km, duration >= 60 s, no impossible speed.
* **Distance:** integral of OBD speed (GPS is privacy-trimmed near trip ends and geofenced,
  so it cannot measure distance).
* **Fuel:** VED Algorithm 1. Fuel Rate [L/h] if present, else
  MAF x (1 + STFT/100 + LTFT/100) / 14.08 (E10 stoichiometric AFR), banks averaged,
  converted with 745 g/L. Fuel energy uses the EPA MPGe convention, 33.7 kWh per US
  gallon = 8.90 kWh/L.
* **Electric energy:** net energy out of the HV battery, sum of -V x I x dt.
* **Complete energy:** ICE/HEV need fuel coverage >= 90 % of trip time; EV battery
  coverage >= 90 %; PHEV both.
* **kWh/100 km:** 100 x SUM(energy_kwh) / SUM(distance_km) over the *same* valid,
  energy-complete trips. Always a ratio of sums, never a mean of trip ratios.
* **Idle share:** SUM(time below 1 km/h) / SUM(idle + moving time).
* **Trips per vehicle:** valid trips / distinct vehicles in the period.

Views: `ved.v_trip`, `ved.agg_trip_daily`, `ved.kpi_energy_by_type`,
`ved.kpi_energy_by_type_hour`, `ved.kpi_vehicle`, `ved.kpi_fleet` (with an `ALL` row),
`ved.v_dq_daily`.

## Results

*Template: fill in after running `make up` and `make kpis`. No numbers have been produced yet.*

Run: date ______, `VED_WEEKS` = ___, seed = ___, trip dates ______ to ______

| Powertrain | Vehicles | Valid trips | Energy-complete trips | km | kWh/100 km | Idle share |
|---|---:|---:|---:|---:|---:|---:|
| ICE | | | | | | |
| HEV | | | | | | |
| PHEV | | | | | | |
| EV | | | | | | |
| ALL | | | | | | |

Data quality (latest ingest date): status ____; corrupt ___ %, missing GPS ___ %,
GPS jump ___ %, impossible speed ___ %, late ___ %, duplicate ___ %.
Compare late / duplicate / impossible speed with `injected_faults.json` in the
`ved-data` volume.

**Sanity bands** (if a value falls outside, suspect the pipeline before the fleet):

* ICE: roughly 50-120 kWh/100 km (about 5.6-13.5 L/100 km).
* EV: roughly 10-30 kWh/100 km at the battery.
* HEV: clearly below ICE.
* PHEV: between EV and HEV, depending on charge-depleting share.

## Design decisions (short)

* **Raw is immutable and complete; everything else is recomputed.** The API only
  validates structure and appends. Dedup, versioning and DQ live in Spark, which
  rewrites curated outputs from all raw data on every run (idempotent by construction).
* **Two duplicate notions.** Exact resends (same content hash) collapse to the first copy;
  distinct versions resolve by partner `sent_at`. They are different operational
  problems and are reported separately.
* **One implementation per rule.** Row rules and per-trip integrals are a pure
  pandas/numpy kernel (`vedpipe/spark/kernel.py`) that Spark runs via `mapInPandas` /
  `applyInPandas` and tests run without a JVM. Record-level logic stays native Spark.
* **Bounded zero-order hold and left Riemann integration** turn multi-rate sparse signals
  into energy without inventing data across dropouts; steps over 10 s are gaps.
* **Additive warehouse.** `agg_trip_daily` stores sums; every ratio is computed after
  aggregation, so slicing by date or type never averages ratios.
* **No FK from fact_trip to dim_vehicle.** Trips for unknown vehicles still load and show
  as `UNKNOWN`.
* **Shared contracts.** `vedpipe.schema` defines signals and trip columns once; the API
  model, Spark schemas, COPY list and SQL are tested against it.

The walkthrough document covers each decision and its trade-offs in detail.

## Limitations and next steps

* **Orchestration:** compose chains a single batch run. Next: Airflow/Dagster with daily
  partitions, retries, SLAs and `--strict` DQ gating before publication.
* **Full recompute:** runtime grows with raw history. Next: Iceberg or Delta tables with
  incremental `MERGE` keyed on (vehicle_id, trip_id), plus raw retention.
* **Object store:** the MinIO image is frozen. Next: a maintained S3-compatible store
  (managed S3/GCS/Azure, or a supported self-hosted build).
* **HEV fuel coverage check:** HEVs report no battery signals, so HEV energy is fuel
  only, which is correct, but HEVs run engine-off for long stretches. Verify that MAF or
  Fuel Rate is still *reported* (as zero) when the engine is off; if it is missing
  instead, coverage drops and HEV trips are excluded from energy KPIs.
* **Fuel fallback:** Algorithm 1's third branch (estimate MAF from absolute load, engine
  speed and displacement) is not implemented, so the 15 ICE and 2 HEV "no fuel" vehicles
  have no fuel energy.
* **PHEVs** need both fuel and battery coverage; pure-electric PHEV trips without engine
  signals are excluded. Next: treat a reported zero fuel rate as coverage.
* **Schema evolution:** DDL is `CREATE ... IF NOT EXISTS`. Next: migrations (Alembic or
  sqitch) once the schema is shared.
* **Security:** static API keys and local credentials. Next: secret manager, key rotation,
  TLS, per-partner rate limits.

## Development

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -e ".[api,sim,spark,load,dashboard,dev]"   # Spark tests also need Java 17
make lint      # ruff check .
make test      # pytest -q (Spark tests skip without pyspark/Java)

# Run pieces against the compose services from the host:
export S3_ENDPOINT_URL=http://localhost:9000 PG_DSN=postgresql://ved:ved@localhost:5432/ved
uvicorn vedpipe.api.main:create_app --factory --reload
python -m vedpipe.spark.job --raw ./data/raw/trips --curated ./data/curated   # local paths
streamlit run dashboard/app.py
```

Make targets: `up`, `pipeline`, `kpis`, `test`, `lint`, `down` (keeps volumes),
`clean` (removes volumes and caches).
