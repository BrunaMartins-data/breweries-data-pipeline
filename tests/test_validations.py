import pytest
from pyspark.sql import SparkSession
from src.pipelines.breweries_transform_silver_notebook import SilverTransform
import os
import yaml
from datetime import datetime, date


"""
Validates:
- Config YAML files exist and are correct
- Referenced scripts are in the expected paths
- Silver layer correctly removes duplicates and nulls
- Schema enforcement matches metadata definitions
"""

CONFIG_PATHS = [
    "configs/breweries_ingestion_pipeline.yml",
    "configs/breweries_gold_pipeline.yml",
]

# ---------------------------------------------------------------------------
# Validate that config YAMLs exist
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("yaml_path", CONFIG_PATHS)
def test_yaml_files_exist(yaml_path):
    """Check if configuration YAML files exist."""
    assert os.path.exists(yaml_path), f"Missing config file: {yaml_path}"


@pytest.mark.parametrize("yaml_path", CONFIG_PATHS)
def test_all_script_paths_exist(yaml_path):
    """Ensure all script paths in YAML configs are valid."""
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
    for stage in config.get("stages", []):
        script_path = stage["parameters"].get("script_path")
        assert os.path.exists(script_path), f"Missing script file: {script_path}"


# ---------------------------------------------------------------------------
# Spark session fixture
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Create a temporary Spark session for testing."""
    return SparkSession.builder.master("local[*]").appName("pytest_silver_validation").getOrCreate()


# ---------------------------------------------------------------------------
# Silver layer validation
# ---------------------------------------------------------------------------
def test_silver_clean_dataframe(spark, tmp_path):
    """Validate Silver layer cleaning logic and schema enforcement."""

    # Simulated input from Bronze layer
    data = [
        {
            "id": "1",
            "name": "Alpha Brewery",
            "brewery_type": "micro",
            "city": "Austin",
            "state": "Texas",
            "country": "United States",
            "updated_at": datetime(2023, 1, 10, 14, 30, 0),
            "ingestion_date": date(2023, 1, 11),
        },
        {
            "id": "1",  # duplicate
            "name": "Alpha Brewery",
            "brewery_type": "micro",
            "city": "Austin",
            "state": "Texas",
            "country": "United States",
            "updated_at": datetime(2023, 1, 10, 14, 30, 0),
            "ingestion_date": date(2023, 1, 11),
        },
        {
            "id": "2",  # valid but with null in required field
            "name": None,
            "brewery_type": "regional",
            "city": "Los Angeles",
            "state": "California",
            "country": "United States",
            "updated_at": datetime(2023, 1, 12, 10, 0, 0),
            "ingestion_date": date(2023, 1, 12),
        },
    ]

    df = spark.createDataFrame(data)

    metadata_path = "/opt/airflow/configs/metadata/breweries_metadata_ingestion.yml"

    transformer = SilverTransform(
        input_path=str(tmp_path),
        output_path=str(tmp_path / "out"),
        metadata_path=metadata_path,
    )

    # Apply cleaning logic
    cleaned = transformer._clean_dataframe(df)

    # Validate deduplication and null removal
    assert cleaned.count() == 1, "Deduplication or null removal failed"
    assert "name" in cleaned.columns, "Column 'name' missing after cleaning"

    # Validate schema enforcement
    with open(metadata_path) as f:
        metadata = yaml.safe_load(f)

    type_mapping = {
        "string": "StringType",
        "double": "DoubleType",
        "timestamp": "TimestampType",
        "date": "DateType"
    }

    expected_schema = {
        col["name"]: type_mapping.get(col["type"].replace("?", ""), "StringType")
        for col in metadata["schema"]
    }

    actual_schema = {
        field.name: type(field.dataType).__name__
        for field in cleaned.schema.fields
    }

    for col_name, expected_type in expected_schema.items():
        assert col_name in actual_schema, f"Column '{col_name}' missing after cleaning"
        assert actual_schema[col_name] == expected_type, (
            f"Incorrect type for '{col_name}': expected {expected_type}, got {actual_schema[col_name]}"
        )

    print("Silver layer cleaning and schema enforcement test passed.")
