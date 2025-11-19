.PHONY: install lint test run demo clean help

# Python executable (use python3 by default, can override with: make PYTHON=python)
PYTHON ?= python3

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make lint       - Run code quality checks"
	@echo "  make test       - Run unit tests"
	@echo "  make demo       - Run demo with synthetic data"
	@echo "  make run        - Execute full pipeline (needs real data)"
	@echo "  make clean      - Remove generated files"

install:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	@echo "Running flake8..."
	flake8 src tests --max-line-length=100 --ignore=E203,W503
	@echo "Running black check..."
	black --check src tests

format:
	@echo "Formatting code with black..."
	black src tests

test:
	@echo "Running pytest..."
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

test-quick:
	$(PYTHON) -m pytest tests/ -q

run:
	$(PYTHON) -m src.cli run

refresh-data:
	$(PYTHON) -m src.cli refresh-data

build-outputs:
	$(PYTHON) -m src.cli build-outputs

demo:
	@echo "Running demo with synthetic data..."
	$(PYTHON) demo.py

clean:
	@echo "Cleaning generated files..."
	rm -rf output/*
	rm -rf data/processed/*
	rm -rf __pycache__ src/__pycache__ tests/__pycache__
	rm -rf .pytest_cache
	rm -rf .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Clean complete!"
