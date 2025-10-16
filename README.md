Breweries Data Pipeline – Apache Airflow

Author: Bruna Martins
Role: Data Engineer
Email: brunatavares81@gmail.com
Version: Final (Production‑Ready)

1. Overview

This project implements a complete data pipeline using Apache Airflow and PySpark, based on the Medallion Architecture (Bronze → Silver → Gold).
It demonstrates robust data engineering practices, including modular orchestration, schema enforcement, observability, and automated data quality validation.

Data is ingested from the Open Brewery DB API, transformed, and aggregated into analytical datasets suitable for reporting and analysis.

2. Architecture Overview
Medallion Flow
API → Bronze (raw JSON)
      ↓
PySpark → Silver (cleaned and normalized Parquet)
      ↓
Aggregations → Gold (aggregated analytical data)
      ↓
Validation → Data Quality Checks

Layer Descriptions
Layer	Purpose	Output Format
Bronze	Ingests raw data from the API using pagination and persists it historically.	JSONL
Silver	Cleans, deduplicates, normalizes, and enforces schema before persisting to Parquet (partitioned by state and country).	Parquet
Gold	Aggregates data into analytical summaries and validates quality rules (nulls, counts, schema).	Parquet

3. Repository Structure
.
├── dags/
│   ├── breweries_ingestion_dag.py         # Bronze + Silver pipeline
│   └── breweries_gold_dag.py              # Gold + Quality pipeline
│
├── src/
│   ├── libs/
│   │   ├── alerts.py                      # Alert callbacks and failure handling
│   │   ├── observability.py               # Logging and monitoring utilities
│   │   └── dag_utils.py                   # DAG generation utilities
│   │
│   └── pipelines/
│       ├── breweries_fetch_bronze_notebook.py
│       ├── breweries_transform_silver_notebook.py
│       ├── breweries_aggregate_gold_notebook.py
│       └── breweries_data_quality_gold.py
│
├── configs/
│   ├── breweries_ingestion_pipeline.yml
│   ├── breweries_gold_pipeline.yml
│   └── metadata/
│       ├── breweries_metadata_ingestion.yml
│       └── breweries_metadata_gold.yml
│
├── tests/
│   ├── test_dag_integrity.py              # Ensures DAGs load correctly
│   └── test_validations.py                # Validates transformations and schema
│
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── entrypoint.sh
└── README.md

4. Environment Setup
Requirements

Docker and Docker Compose installed

(Optional) Python 3.12+ for local test execution

Build the environment
make build

Start Airflow and dependencies
make up


Access Airflow UI at:
http://localhost:8080

User: airflow
Password: airflow

Stop the environment
make down

Full reset (containers, volumes, cache)
make reset

Access the Airflow container
make exec

5. DAG Execution
Trigger DAGs manually
docker exec -it breweries_airflow airflow dags trigger breweries_ingestion_daily_at_07
docker exec -it breweries_airflow airflow dags trigger breweries_gold_daily_at_07_30

DAG Details
DAG	Tasks	Description
breweries_ingestion_daily_at_07	fetch_data_bronze, transform_silver	Extracts and transforms raw brewery data.
breweries_gold_daily_at_07_30	aggregate_gold, validate_gold_quality	Aggregates data into analytical views and validates quality rules.
View logs
make logs

6. Testing and Validation

Automated tests ensure integrity and consistency across the pipeline.

Types of Tests
Test File	Purpose
test_dag_integrity.py	Confirms all DAGs load successfully and contain required tasks.
test_validations.py	Validates schema enforcement, deduplication, and null removal logic for the Silver layer.
Run tests inside the container
pytest tests/ --disable-warnings -v

7. Observability and Monitoring

The project integrates structured observability through the following mechanisms:

Component	Purpose
observability.py	Logs task start/end events, durations, and error messages.
alerts.py	Sends failure alerts via email or other integrations.
Airflow Logging	Centralized logs for all tasks under /opt/airflow/logs/.

Optional integrations:

MLflow for metric tracking (e.g., records processed per stage).

Prometheus + Alertmanager for task success/failure metrics.

8. Design Decisions
Silver Layer Data Quality

Data quality checks for the Silver layer are embedded directly in the transformation logic.
This ensures that invalid or duplicate records are removed before persistence, preventing downstream inconsistencies.

Task Naming Convention
Layer	Task	Description
Bronze	fetch_data_bronze	Extracts data from API with pagination.
Silver	transform_silver	Cleans, deduplicates, and applies schema enforcement.
Gold	aggregate_gold	Aggregates metrics by region and type.
Gold	validate_gold_quality	Executes quality validation rules on aggregated data.
Orchestration

Pipelines are defined declaratively via YAML.

DAGs are dynamically generated using shared utilities.

All tasks include retries, backoff policies, and structured logging.

9. Containerization and Automation

All Docker and orchestration commands are managed via a Makefile for reproducibility.

Command	Description
make build	Build the Docker image
make up	Start Airflow and dependencies
make down	Stop containers
make reset	Remove containers, volumes, and cache
make logs	Stream logs in real time
make exec	Open a shell inside the Airflow container
make test	Validate permissions and run unit tests
make sync	Sync DAGs and source code into the container

10. Future Improvements

Integrate CI/CD with GitHub Actions to automate pytest on pull requests.

Extend Prometheus metrics to Grafana dashboards.

Expand data quality validations with business rules for the Gold layer.

Introduce data lineage tracking via OpenLineage integration.

11. Author

Bruna Martins
Data Engineer
Email: brunatavares81@gmail.com
Location: Brazil