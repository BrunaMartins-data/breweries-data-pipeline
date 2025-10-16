import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import importlib.util
import yaml
import pendulum
from src.libs.alerts import notify_failure
from src.libs.dag_utils import wrap_task_with_logging, log_task_event


class PipelineDAG:
    """Dynamic DAG builder for ingestion and transformation (Bronze → Silver)."""

    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path

    def _resolve_yaml_path(self):
        """Ensure the YAML path always resolves inside /opt/airflow/configs."""
        base_dir = "/opt/airflow"
        full_path = os.path.join(base_dir, "configs", self.yaml_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"YAML not found: {full_path}")
        return full_path

    def _load_yaml(self):
        """Load YAML configuration."""
        yaml_path = self._resolve_yaml_path()
        with open(yaml_path, "r") as f:
            return yaml.safe_load(f)

    def _load_python_callable(self, script_path: str):
        """Dynamically import a Python script and return its main() function."""
        spec = importlib.util.spec_from_file_location("module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, "main"):
            raise AttributeError(f"The script {script_path} must contain a main() function.")
        return module.main

    def build(self):
        """Build the ingestion DAG dynamically from YAML."""
        config = self._load_yaml()
        local_tz = pendulum.timezone(config["dag"].get("timezone", "UTC"))

        # Parse start_date safely
        start_date_str = str(config["dag"].get("start_date", "2025-10-12"))
        try:
            start_date = pendulum.parse(start_date_str).replace(tzinfo=local_tz)
        except Exception:
            start_date = pendulum.datetime(2025, 10, 12, tz=local_tz)

        default_args = {
            "owner": "bruna",
            "depends_on_past": False,
            "start_date": start_date,
            "email_on_failure": True,
            "email_on_retry": False,
            "email": ["brunatavares81@gmail.com"],
            "on_failure_callback": notify_failure,
            "retries": 3,
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(minutes=10),
            "retry_delay": timedelta(minutes=2),
            "execution_timeout": timedelta(minutes=30),
        }

        dag = DAG(
            dag_id=config["dag"]["dag_id"],
            description=config["dag"].get("description", "ETL ingestion pipeline"),
            default_args=default_args,
            schedule_interval=config["dag"].get("schedule_interval", None),
            catchup=False,
            tags=["bronze", "silver", "ingestion", "breweries"],
        )

        previous_task = None

        for stage in config["stages"]:
            task_id = stage["task_id"]
            script_path = os.path.join("/opt/airflow/src/pipelines", stage["script"])
            stage_params = stage.get("parameters", {}).copy()

            # Log structured task start
            log_task_event(task_id, "initializing", {"script_path": script_path})

            func = self._load_python_callable(script_path)
            wrapped_callable = wrap_task_with_logging(task_id, func)

            task = PythonOperator(
                task_id=task_id,
                python_callable=wrapped_callable,
                op_kwargs={**stage_params, "execution_date": "{{ ds }}"},
                dag=dag,
            )

            if previous_task:
                previous_task >> task
            previous_task = task

        return dag


# Instantiate DAG
dag = PipelineDAG("breweries_ingestion_pipeline.yml").build()
