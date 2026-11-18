# Developer and operator entry points. `make up` is the only command needed for a demo.
COMPOSE ?= docker compose
PSQL = $(COMPOSE) exec -T postgres psql -U ved -d ved -P pager=off

.PHONY: up pipeline kpis test lint down clean

up:  ## build images and run the whole pipeline, then keep api/dashboard running
	$(COMPOSE) up --build -d
	@echo "Dashboard http://localhost:8501 | API http://localhost:8000/docs | MinIO http://localhost:9001"

pipeline:  ## re-run curation, load and DQ report against the current raw bucket
	$(COMPOSE) run --rm --no-deps spark-job
	$(COMPOSE) run --rm --no-deps loader
	$(COMPOSE) run --rm --no-deps dq-report

kpis:  ## print the headline KPI views
	$(PSQL) -c "SELECT vehicle_type, n_vehicles, n_trips, round(trips_per_vehicle::numeric, 1) AS trips_per_vehicle, round(distance_km::numeric) AS km, round(kwh_per_100km::numeric, 1) AS kwh_per_100km, round(100 * idle_share::numeric, 1) AS idle_pct FROM ved.kpi_fleet ORDER BY vehicle_type = 'ALL', vehicle_type"
	$(PSQL) -c "SELECT vehicle_type, n_vehicles, n_energy_trips, round(energy_distance_km::numeric) AS km, round(kwh_per_100km::numeric, 1) AS kwh_per_100km FROM ved.kpi_energy_by_type ORDER BY vehicle_type"
	$(PSQL) -c "SELECT ingest_date, status, n_records, n_corrupt, n_trips_loaded, round(100 * late_rate::numeric, 2) AS late_pct, round(100 * duplicate_rate::numeric, 2) AS dup_pct FROM ved.v_dq_daily ORDER BY ingest_date"

test:  ## unit + Spark tests (Spark tests skip without pyspark/Java)
	pytest -q

lint:
	ruff check .

down:  ## stop containers, keep data volumes
	$(COMPOSE) down

clean:  ## stop containers and delete all data volumes and local caches
	$(COMPOSE) down -v --remove-orphans
	rm -rf .pytest_cache .ruff_cache build dist src/*.egg-info reports
