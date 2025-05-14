.PHONY: up down logs test format lint typecheck check

# Docker
up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f clickhouse

# Тесты
test:
	pytest -v --tb=short tests/

# Линтеры и типизация
lint:
	flake8 clickhouse_insert

format:
	black clickhouse_insert

typecheck:
	mypy clickhouse_insert

# Быстрая проверка всего
check: lint typecheck test
