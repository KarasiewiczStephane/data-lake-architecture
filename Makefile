.PHONY: install test lint clean run up down init shell dashboard

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ --fix
	ruff format src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

run:
	python -m src.main

up:
	docker-compose up -d

down:
	docker-compose down

init: up
	python -m src.cli init

shell:
	docker-compose exec datalake bash

dashboard:
	streamlit run src/dashboard/app.py
