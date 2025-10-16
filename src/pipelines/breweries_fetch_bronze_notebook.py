import os
import json
import requests
import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.libs.observability import init_logging, end_logging


class BronzeIngestion:
    """
    Ingests raw data from the Open Brewery API and writes it to the Bronze layer.
    Adds an ingestion_date column for traceability and supports atomic file writes.
    """

    def __init__(self, source_url: str, target_path: str, stage_name: str = "bronze_ingest"):
        self.source_url = source_url
        self.target_path = target_path
        self.stage_name = stage_name
        self.spark = SparkSession.builder.appName(stage_name).getOrCreate()
        self.meta = None

    # -----------------------------------------------
    # API Fetch (paginação com limite e backoff)
    # -----------------------------------------------
    def _fetch_api_data(self, per_page: int = 50, max_pages: int = None, retries: int = 3, backoff: int = 2) -> list:
        """Fetch paginated data from the API endpoint."""
        all_data = []
        page = 1

        while True:
            try:
                response = requests.get(
                    self.source_url,
                    params={"page": page, "per_page": per_page},
                    timeout=30,
                )
            except Exception as e:
                if retries > 0:
                    print(f" Request failed (attempts left: {retries}). Retrying in {backoff}s...")
                    time.sleep(backoff)
                    return self._fetch_api_data(per_page, max_pages, retries - 1, backoff * 2)
                else:
                    raise e

            if response.status_code == 429:
                print(f" Rate limit hit at page {page}. Waiting before retry...")
                time.sleep(backoff)
                continue

            if response.status_code != 200:
                raise Exception(f"API request failed (status={response.status_code}) at page {page}")

            data = response.json()
            if not data:
                break

            all_data.extend(data)
            print(f"Fetched page {page} with {len(data)} records.")

            if max_pages and page >= max_pages:
                break
            page += 1

        return all_data

    # -----------------------------------------------
    # Escrita atômica (JSON Lines)
    # -----------------------------------------------
    def _save_raw_json(self, all_data):
        """Safely writes the raw data to JSONL format inside container volume."""
        os.makedirs(self.target_path, exist_ok=True)
        final_path = os.path.join(self.target_path, "raw.jsonl")
        tmp_path = final_path + ".tmp"

        # Escreve primeiro em um arquivo temporário
        with open(tmp_path, "w") as f:
            for row in all_data:
                f.write(json.dumps(row) + "\n")

        # Move apenas após a escrita completa
        os.rename(tmp_path, final_path)
        print(f"[INFO] Raw JSON written atomically to {final_path}")
        return final_path

    # -----------------------------------------------
    # Processamento com Spark (sem Pandas)
    # -----------------------------------------------
    def _add_ingestion_date(self, json_path: str):
        """Reads JSONL file directly with Spark and adds ingestion_date."""
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Expected file not found at {json_path}")

        print(f"[INFO] Reading raw JSON with Spark directly...")
        df = (
            self.spark.read.option("multiline", "false")
            .json(json_path)
            .withColumn("ingestion_date", lit(str(date.today())))
        )

        # Cria diretório versionado (run_<hora>)
        bronze_output_dir = os.path.join(os.path.dirname(json_path), f"run_{datetime.now().strftime('%H%M%S')}")
        print(f"[INFO] Writing enriched data to {bronze_output_dir}")

        df.write.mode("overwrite").json(bronze_output_dir)
        print(f"[INFO] Bronze layer successfully written to {bronze_output_dir}")
        return df

    # -----------------------------------------------
    # Execução principal com logging observável
    # -----------------------------------------------
    def run(self):
        """Main entrypoint for the Bronze ingestion stage."""
        self.meta = init_logging(
            self.stage_name, {"source_url": self.source_url, "target_path": self.target_path}
        )

        try:
            all_data = self._fetch_api_data()
            json_path = self._save_raw_json(all_data)
            df = self._add_ingestion_date(json_path)

            record_count = df.count()
            print(f"Ingestion complete: {record_count} records on {date.today()}")
            end_logging(self.meta, {"records_ingested": record_count})

        except Exception as e:
            end_logging(self.meta, error=e)
            raise
# -----------------------------------------------
# Entrypoint (Airflow-compatible)
# -----------------------------------------------
def main(**context):
    """Entrypoint required by Airflow DAG."""
    execution_date = context.get("execution_date", datetime.now().strftime("%Y-%m-%d"))
    params = {
        "source_url": "https://api.openbrewerydb.org/v1/breweries",
        "target_path": f"/opt/airflow/data/bronze/breweries/{execution_date}",
    }

    ingestion = BronzeIngestion(**params)
    ingestion.run()


if __name__ == "__main__":
    main()
