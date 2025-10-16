import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from src.libs.observability import init_logging, end_logging


class GoldAggregation:
    """
    Aggregates curated Silver data into analytical views for the Gold layer.
    Produces multiple summary tables and consolidates them into a unified dataset.
    """

    def __init__(self, input_path: str, output_path: str, stage_name: str = "gold_aggregation"):
        self.input_path = input_path
        self.output_path = output_path
        self.stage_name = stage_name
        self.spark = (
            SparkSession.builder.appName(stage_name)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        self.meta = None

    def _read_silver_data(self):
        """Read cleaned Silver data from partitioned directory."""
        print(f"[INFO] Reading Silver data from {self.input_path}")
        df = self.spark.read.parquet(self.input_path)
        print(f"[INFO] Loaded {df.count()} records from Silver layer.")
        return df

    def _aggregate_views(self, df):
        """Generate multiple aggregated analytical views for the Gold layer."""

        print("[INFO] Creating Gold aggregations...")

        df_country_type = (
            df.groupBy("country", "brewery_type")
              .agg(count("*").alias("total_breweries"))
              .withColumn("aggregation", lit("by_country_type"))
        )

        df_state_city_type = (
            df.groupBy("state", "city", "brewery_type")
              .agg(count("*").alias("total_breweries"))
              .withColumn("aggregation", lit("by_state_city_type"))
        )

        df_type_city_state = (
            df.groupBy("brewery_type", "city", "state")
              .agg(count("*").alias("total_breweries"))
              .withColumn("aggregation", lit("by_type_city_state"))
        )

        df_union = (
            df_country_type
            .unionByName(df_state_city_type, allowMissingColumns=True)
            .unionByName(df_type_city_state, allowMissingColumns=True)
            .withColumn("transformation_date", lit(date.today().isoformat()))
        )

        record_count = df_union.count()
        print(f"[INFO] Consolidated Gold dataset has {record_count} records.")

        df_union.write.mode("overwrite").parquet(self.output_path)
        print(f"[INFO] ✅ Gold layer successfully written to {self.output_path}")

        return record_count

    def run(self):
        """Main execution method for the Gold transformation stage."""
        self.meta = init_logging(self.stage_name, {"input_path": self.input_path, "output_path": self.output_path})

        try:
            df = self._read_silver_data()
            record_count = self._aggregate_views(df)
            end_logging(self.meta, {"records_processed": record_count})
        except Exception as e:
            end_logging(self.meta, error=e)
            raise


def main(**kwargs):
    """Entrypoint required by Airflow DAG."""
    execution_date = kwargs.get("execution_date", None)
    input_path = kwargs.get("input_path", f"/opt/airflow/data/silver/breweries/{execution_date}/")
    output_path = kwargs.get("output_path", f"/opt/airflow/data/gold/breweries/{execution_date}/")

    aggregator = GoldAggregation(input_path=input_path, output_path=output_path)
    aggregator.run()


if __name__ == "__main__":
    main()
