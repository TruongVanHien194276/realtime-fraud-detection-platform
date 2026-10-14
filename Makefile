RAW_PATH ?= data/raw/PS_20174392719_1491204439457_log.csv
SPARK_WAREHOUSE ?= data/lakehouse
WAREHOUSE_SAMPLE ?= data/tmp/week2-benchmark-100k-v1
DBT_FLAGS ?=
SPARK_SUBMIT = docker compose run --rm spark spark-submit --master 'local[2]' --driver-memory 1g --jars /opt/iceberg/iceberg-spark-runtime.jar
SPARK_SUBMIT_JDBC = docker compose run --rm spark spark-submit --master 'local[2]' --driver-memory 1g --jars /opt/iceberg/iceberg-spark-runtime.jar,/opt/jdbc/postgresql.jar

.PHONY: build shell profile samples smoke-samples lint format format-check test check spark-build bronze-100k bronze-1m silver verify-lakehouse check-spark warehouse-up warehouse-export-100k warehouse-merge warehouse-indexes dbt-build dbt-docs

build:
	docker compose build dev

shell:
	docker compose run --rm dev bash

profile:
	docker compose run --rm dev python scripts/profile_dataset.py --input "$(RAW_PATH)"

smoke-samples:
	docker compose run --rm dev python scripts/create_samples.py --input "$(RAW_PATH)" --integration-size 10000 --development-size 5000 --unit-size 1000 --output-directory data/tmp/smoke-samples

samples:
	docker compose run --rm dev python scripts/create_samples.py --input "$(RAW_PATH)"

lint:
	docker compose run --rm dev ruff check --no-cache .

format:
	docker compose run --rm dev ruff format .

format-check:
	docker compose run --rm dev ruff format --check .

test:
	docker compose run --rm dev pytest -p no:cacheprovider tests/unit

check: lint format-check test

spark-build:
	docker compose build spark

bronze-100k:
	$(SPARK_SUBMIT) spark/bronze_ingestion.py --input data/samples/representative_100k.parquet --format parquet --batch-id week2-dev-100k --warehouse $(SPARK_WAREHOUSE)

bronze-1m:
	$(SPARK_SUBMIT) spark/bronze_ingestion.py --input data/samples/representative_1m.parquet --format parquet --batch-id week2-integration-1m --warehouse $(SPARK_WAREHOUSE)

silver:
	$(SPARK_SUBMIT) spark/silver_transform.py --warehouse $(SPARK_WAREHOUSE)

verify-lakehouse:
	$(SPARK_SUBMIT) spark/verify_lakehouse.py --warehouse $(SPARK_WAREHOUSE)

check-spark:
	docker compose run --rm spark pytest -q tests/data_quality tests/integration/test_batch_lakehouse.py

# Chạy từng pha riêng để không giữ Spark và dbt đồng thời trên máy RAM 8 GB.
warehouse-up:
	docker compose --profile warehouse up -d --wait postgres

warehouse-export-100k: warehouse-up
	$(SPARK_SUBMIT_JDBC) spark/export_silver_to_postgres.py --warehouse $(WAREHOUSE_SAMPLE)

warehouse-merge: warehouse-up
	docker compose --profile warehouse exec -T postgres psql -v ON_ERROR_STOP=1 -U fraud -d fraud_dw < sql/warehouse/002_merge_incoming.sql

warehouse-indexes: warehouse-up
	docker compose --profile warehouse exec -T postgres psql -v ON_ERROR_STOP=1 -U fraud -d fraud_dw < sql/warehouse/003_indexes.sql

dbt-build: warehouse-up
	docker compose --profile warehouse run --rm dbt dbt build --no-partial-parse $(DBT_FLAGS)
	$(MAKE) warehouse-indexes

dbt-docs: warehouse-up
	docker compose --profile warehouse run --rm dbt dbt docs generate --no-partial-parse
