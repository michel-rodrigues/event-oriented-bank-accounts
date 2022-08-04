init:
	@pip3 install --upgrade pip setuptools wheel
	@pip3 install --upgrade poetry
	@poetry install
	@pre-commit install
	@pre-commit run --all-files

.PHONY: isort
isort:
	@poetry run isort --sp pyproject.toml .

.PHONY: isort-check
isort-check:
	@poetry run isort --check --sp pyproject.toml .

.PHONY: flake8
flake8:
	@poetry run flake8 .

.PHONY: blue
blue:
	@poetry run blue -v .

.PHONY: blue-check
blue-check:
	@poetry run blue -v --check .

.PHONY: format
format: blue isort

.PHONY: lint
lint: isort-check flake8 blue-check

run:
	@poetry install
	@poetry run env $(shell grep -v ^\# .env | xargs) uvicorn src.main:app --reload --port 8080

test-all:
	@poetry run env $(shell grep -v ^\# .env | xargs) pytest

poetry-export:
	@poetry export --without-hashes --no-ansi --no-interaction --format requirements.txt --output requirements.txt

build-image: poetry-export
	@docker build -t bank-accounts:latest .

run-docker: build-image
	@docker run --rm -p '8080:8080' bank-accounts:latest
