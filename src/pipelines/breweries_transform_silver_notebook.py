import os
import yaml
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType
)
from pyspark.sql.functions import col, trim, lower
from src.libs.observability import init_logging, end_logging


class SilverTransform:
    """
    Cleans and standardizes Bronze layer data to produce the Silver layer.
    Handles column normalization, deduplication, type enforcement, and partitioning.
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        metadata_path: str,
        output_format: str = "parquet",
        partition_by: list = ["state", "country"],
        stage_name: str = "silver_transform"
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.metadata_path = metadata_path
        self.output_format = output_format
        self.partition_by = partition_by
        self.stage_name = stage_name
        self.spark = (
            SparkSession.builder.appName(stage_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        self.meta = None

    def _load_metadata(self):
        if not os.path.exists(self.metadata_path):
            raise FileNotFoundError(f"Metadata not found at {self.metadata_path}")
        with open(self.metadata_path, "r") as f:
            return yaml.safe_load(f)

    def _build_schema(self, meta: dict):
        type_mapping = {
            "string": StringType(),
            "double": DoubleType(),
            "timestamp": TimestampType(),
            "date": DateType()
        }
        fields = [
            StructField(
                col["name"],
                type_mapping.get(col["type"].replace("?", ""), StringType()),
                col.get("nullable", True)
            )
            for col in meta["schema"]
        ]
        return StructType(fields)

    def _read_bronze(self, schema: StructType):
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input path not found: {self.input_path}")
        print(f"[INFO] Reading Bronze data with schema enforcement...")
        return self.spark.read.schema(schema).json(self.input_path)

    def _clean_dataframe(self, df):
        print("[INFO] Cleaning and normalizing DataFrame...")
        df = (
            df.dropDuplicates(["id"])
              .dropna(subset=["id", "name", "state", "country"])
              .withColumn("name", trim(lower(col("name"))))
              .withColumn("city", trim(lower(col("city"))))
              .withColumn("state", trim(lower(col("state"))))
              .withColumn("country", trim(lower(col("country"))))
              .withColumn("brewery_type", trim(lower(col("brewery_type"))))
        )
        cleaned_count = df.count()
        print(f"[INFO] Cleaned DataFrame has {cleaned_count} records.")
        return df

    def _save_dataframe(self, df):
        os.makedirs(self.output_path, exist_ok=True)
        print(f"[INFO] Writing Silver data to {self.output_path} as {self.output_format.upper()}...")
        writer = df.write.mode("overwrite")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.format(self.output_format).save(self.output_path)
        print(f"[INFO] Silver layer successfully written to {self.output_path}")

    def run(self):
        self.meta = init_logging(
            self.stage_name,
            {
                "input_path": self.input_path,
                "output_path": self.output_path,
                "format": self.output_format,
                "partition_by": self.partition_by,
            },
        )

        try:
            meta = self._load_metadata()
            schema = self._build_schema(meta)
            df = self._read_bronze(schema)
            cleaned_df = self._clean_dataframe(df)
            self._save_dataframe(cleaned_df)

            record_count = cleaned_df.count()
            print(f"[INFO] Total records written to Silver: {record_count}")
            end_logging(self.meta, {"records_written": record_count})

        except Exception as e:
            end_logging(self.meta, error=e)
            raise


def main(**kwargs):
    execution_date = kwargs.get("execution_date", datetime.now().strftime("%Y-%m-%d"))
    params = {
        "input_path": f"/opt/airflow/data/bronze/breweries/{execution_date}/",
        "output_path": f"/opt/airflow/data/silver/breweries/{execution_date}/",
        "metadata_path": "/opt/airflow/configs/metadata/breweries_metadata_ingestion.yml",
        "output_format": "parquet",
        "partition_by": ["state", "country"]
    }
    SilverTransform(**params).run()


if __name__ == "__main__":
    main()
