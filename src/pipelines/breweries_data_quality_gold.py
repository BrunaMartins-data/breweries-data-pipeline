import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def main(input_path: str, quality_rules: list = None, execution_date: str = None, **kwargs):
    print("Starting Data Quality validation for Gold layer")

    spark = SparkSession.builder.appName("GoldDataQuality").getOrCreate()
    print(f"Reading Gold data from {input_path}")
    df = spark.read.parquet(input_path)

    results = []

    # Default rules if not passed
    quality_rules = quality_rules or [
        {"rule": "No null values in brewery_type", "column": "brewery_type", "type": "not_null"},
        {"rule": "No null values in city", "column": "city", "type": "not_null"},
        {"rule": "Count > 0 for all states", "column": "total_breweries", "type": "greater_than_zero"},
    ]

    for rule in quality_rules:
        col_name = rule["column"]
        rule_name = rule.get("rule") or rule.get("name", "Unnamed rule")
        rule_type = rule["type"]

        if rule_type == "not_null":
            invalid_count = df.filter(col(col_name).isNull()).count()
        elif rule_type == "greater_than_zero":
            invalid_count = df.filter(col(col_name) <= 0).count()
        else:
            invalid_count = 0

        passed = invalid_count == 0
        results.append({"rule": rule_name, "passed": passed, "invalid_count": invalid_count})

    # Save report
    report_path = f"/opt/airflow/data/quality/breweries/{execution_date or 'manual'}/gold_report.json"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Data Quality Report saved at {report_path}")
    print(json.dumps(results, indent=2))

    spark.stop()
    print("Data Quality checks completed successfully.")
