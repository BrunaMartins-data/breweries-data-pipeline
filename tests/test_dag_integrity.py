import os
import pytest
from airflow.models import DagBag

""""Ensure all DAGs are valid and load without import errors;

Validate that the expected tasks exist and follow the standard naming convention;

Prevent deployment if there is a syntax error or broken dependency."""
DAG_PATH = "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dag_bag():
    """Loads all DAGs for validation"""
    return DagBag(dag_folder=DAG_PATH, include_examples=False)


def test_import_dags(dag_bag):
    """Validates that all DAGs were imported correctly."""
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"


@pytest.mark.parametrize(
    "dag_id",
    ["breweries_ingestion_daily_at_07", "breweries_gold_daily_at_07_30"]
)
def test_dag_loaded(dag_bag, dag_id):
    """Checks if the expected DAGs are loaded."""
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} não encontrada"
    assert dag.default_args["owner"] == "bruna"


@pytest.mark.parametrize(
    "dag_id, expected_tasks",
    [
        ("breweries_ingestion_daily_at_07", ["fetch_data_bronze", "transform_silver"]),
        ("breweries_gold_daily_at_07_30", ["aggregate_gold", "validate_gold_quality"]),
    ],
)
def test_dag_structure(dag_bag, dag_id, expected_tasks):
    """Validates if parent tasks exist and follow the pattern."""
    dag = dag_bag.get_dag(dag_id)
    assert set(expected_tasks).issubset(dag.task_ids), f"DAG {dag_id} without all the expected tasks."
