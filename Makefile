# =========================================================
# Breweries Airflow Project - Makefile
# Author: Bruna Martins
# Version: Final (Production Ready)
# =========================================================

PROJECT_NAME=breweries_project
COMPOSE_FILE=docker-compose.yml
CONTAINER_NAME=breweries_airflow

# =========================================================
# Build & Setup
# =========================================================
.PHONY: build up down reset logs ps exec test sync

## Build the Docker image
build:
	@echo "Building Docker image for the project..."
	docker compose -f $(COMPOSE_FILE) build --no-cache

## Start the environment (Postgres + Airflow)
up:
	@echo "Starting Airflow and dependencies..."
	docker compose -f $(COMPOSE_FILE) up -d
	docker ps

## Stop and remove containers (keep volumes)
down:
	@echo "Stopping containers (volumes will be preserved)..."
	docker compose -f $(COMPOSE_FILE) down

## Full reset (remove containers, volumes, and cache)
reset:
	@echo "Resetting full environment (containers, volumes, cache)..."
	docker compose -f $(COMPOSE_FILE) down -v
	docker system prune -af --volumes
	sudo chown -R 50000:0 data logs mlruns || true
	sudo find data logs mlruns -type d -exec chmod 775 {} \; || true
	sudo find data logs mlruns -type f -exec chmod 664 {} \; || true
	@echo "Environment cleaned and ready for rebuild."

## Show Airflow container logs
logs:
	@echo "Showing logs for container: $(CONTAINER_NAME)..."
	docker logs -f $(CONTAINER_NAME)

## Show running containers
ps:
	docker ps

## Access Airflow container (bash)
exec:
	docker exec -it $(CONTAINER_NAME) bash

## Test write permissions inside the Airflow container
test:
	@echo "Testing write permissions in the container..."
	docker exec -it $(CONTAINER_NAME) bash -lc "id && touch /opt/airflow/data/bronze/_perm_test && echo 'Write permissions OK for Bronze layer.'"
	docker exec -it $(CONTAINER_NAME) bash -lc "rm -f /opt/airflow/data/bronze/_perm_test"
	@echo "Running tests inside the container..."
	docker exec -it $(CONTAINER_NAME) bash -lc "pytest -v --disable-warnings"


## Sync DAGs and source files into the container without full rebuild
sync:
	@echo "Syncing DAGs and source files to the container..."
	docker cp dags/. $(CONTAINER_NAME):/opt/airflow/dags/
	docker cp src/. $(CONTAINER_NAME):/opt/airflow/src/
	docker exec -it $(CONTAINER_NAME) bash -lc "airflow dags list"
	@echo "DAGs successfully synced to the container."
