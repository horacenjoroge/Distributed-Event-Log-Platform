.PHONY: help install install-dev proto clean test lint format docker-build docker-up docker-down

help:
	@echo "DistributedLog - Makefile Commands"
	@echo ""
	@echo "Development:"
	@echo "  install          Install production dependencies"
	@echo "  install-dev      Install development dependencies"
	@echo "  proto            Generate Python code from Protocol Buffers"
	@echo "  clean            Remove generated files and caches"
	@echo ""
	@echo "Quality:"
	@echo "  test             Run test suite"
	@echo "  lint             Run linters (ruff, mypy)"
	@echo "  format           Format code with black"
	@echo "  pre-commit       Run pre-commit hooks on all files"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build     Build Docker images"
	@echo "  docker-up        Start all services"
	@echo "  docker-down      Stop all services"
	@echo "  docker-logs      View logs from all services"
	@echo ""

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt
	pre-commit install

proto:
	./scripts/generate_proto.sh

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*_pb2.py" -delete
	find . -name "*_pb2.pyi" -delete
	find . -name "*_pb2_grpc.py" -delete
	rm -rf dist/ build/

test:
	pytest distributedlog/tests/ -v --cov=distributedlog --cov-report=html --cov-report=term

lint:
	ruff check distributedlog/
	mypy distributedlog/

format:
	black distributedlog/
	ruff check --fix distributedlog/

pre-commit:
	pre-commit run --all-files

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d
	@echo "Services started! Access:"
	@echo "  Broker 0: localhost:9092 (gRPC: 9093, Metrics: 9094)"
	@echo "  Broker 1: localhost:9192 (gRPC: 9193, Metrics: 9194)"
	@echo "  Broker 2: localhost:9292 (gRPC: 9293, Metrics: 9294)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker system prune -f
