.PHONY: help setup run test clean docker-build docker-run

PYTHON ?= python3
VENV   ?= .venv
ACT     = . $(VENV)/bin/activate

help:
	@echo "make setup         create venv and install deps"
	@echo "make run           run the pipeline with config.yaml"
	@echo "make test          run unit tests"
	@echo "make clean         remove venv and output artifacts"
	@echo "make docker-build  build the Docker image"
	@echo "make docker-run    run the pipeline inside Docker"

setup:
	$(PYTHON) -m venv $(VENV)
	$(ACT) && pip install --upgrade pip && pip install -e ".[dev]"

run:
	$(ACT) && python -m taxi_top_trips

test:
	$(ACT) && pytest -v

clean:
	rm -rf $(VENV) output/top_trips output/summary.parquet data/raw __pycache__ \
	       src/taxi_top_trips/__pycache__ tests/__pycache__ .pytest_cache *.egg-info

docker-build:
	docker build -t taxi-top-trips:latest .

docker-run:
	docker run --rm -v $(PWD)/output:/app/output -v $(PWD)/config.yaml:/app/config.yaml \
	    taxi-top-trips:latest
