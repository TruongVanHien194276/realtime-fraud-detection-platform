RAW_PATH ?= data/raw/PS_20174392719_1491204439457_log.csv

.PHONY: build shell profile samples smoke-samples lint format format-check test check

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
	docker compose run --rm dev pytest -p no:cacheprovider

check: lint format-check test
