.PHONY: all install build run test lint clean docker-up docker-down migrate

all: build test

install:
	pip install -r backend/requirements.txt
	cd frontend && npm install

build:
	cd frontend && npm run build

run:
	uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest backend/tests -v

lint:
	flake8 backend/app --max-line-length=120

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf frontend/dist

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v
