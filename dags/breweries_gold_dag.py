import os
from src.libs.alerts import notify_failure
from src.libs.observability import init_logging, end_logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.session import provide_session
from prometheus_client import Counter, REGISTRY
from datetime import timedelta
import importlib.util
import yaml
import pendulum


# =========================================================
# Safe initialization of Prometheus metrics
# =========================================================
try:
    metric_names = [
        getattr(m, "_name", None)
        for m in REGISTRY._names_to_collectors.values()
        if hasattr(m, "_name")
    ]
    if "airflow_gold_success_total" not in metric_names:
        gold_success_counter = Counter(
            "airflow_gold_success_total", "Total number of Gold DAG successful runs"
        )
except Exception:
    gold_success_counter = Counter(
        "airflow_gold_success_total", "Total number of Gold DAG successful runs"
    )

try:
    gold_failure_counter = Counter(
        "airflow_gold_failure_total", "Total number of Gold DAG failed runs"
    )
except Exception:
    pass


# =========================================================
# Dynamic DAG Builder Class for Gold Layer
# =========================================================
class GoldPipelineDAG:
    """Dynamic DAG builder for Gold layer processing."""

    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path

    def _resolve_yaml_path(self):
        """
        Resolve the YAML path safely for execution inside or outside Docker.
        Ensures the absolute path always points to /opt/airflow/configs/...
        """
        base_dir = "/opt/airflow"
        full_path = os.path.join(base_dir, "configs", self.yaml_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"YAML not found: {full_path}")
        return full_path

    def _load_yaml(self):
        """Load the YAML configuration file for the DAG."""
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

    # =========================================================
    # Build DAG
    # =========================================================
    def build(self):
        """Build the DAG dynamically using the configuration YAML."""
        config = self._load_yaml()
        local_tz = pendulum.timezone(config["dag"].get("timezone", "UTC"))

        start_date = config["dag"].get("start_date")
        if start_date:
            start_date = pendulum.parse(str(start_date)).replace(tzinfo=local_tz)
        else:
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
            description=config["dag"].get("description", "Gold pipeline DAG"),
            default_args=default_args,
            schedule_interval=config["dag"].get("schedule_interval", None),
            catchup=False,
            tags=["gold", "aggregation", "breweries"],
        )

        # =========================================================
        # External Dependency: Wait for successful ingestion DAG
        # =========================================================
        @provide_session
        def latest_successful_ingestion(execution_date, session=None):
            """Return the latest successful ingestion DAG run execution date."""
            dagruns = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "breweries_ingestion_daily_at_07")
                .filter(DagRun.state == "success")
                .order_by(DagRun.execution_date.desc())
                .all()
            )
            return dagruns[0].execution_date if dagruns else None

        wait_for_ingestion = ExternalTaskSensor(
            task_id="wait_for_ingestion",
            external_dag_id="breweries_ingestion_daily_at_07",
            external_task_id="transform_silver",
            mode="reschedule",
            execution_date_fn=latest_successful_ingestion,
            timeout=600,
            poke_interval=30,
            dag=dag,
        )

        previous_task = wait_for_ingestion

        # =========================================================
        # Stage Processing (Aggregate + Data Quality)
        # =========================================================
        for stage in config["stages"]:
            task_id = stage["task_id"]
            script_path = os.path.join("/opt/airflow/src/pipelines", stage["script"])
            stage_params = stage.get("parameters", {}).copy()

            def create_task_callable(script_path, task_id, stage_params):
                """Create a task callable that wraps the execution and logging logic."""
                def _callable(**kwargs):
                    meta = init_logging(task_id, stage_params)
                    try:
                        func = self._load_python_callable(script_path)
                        result = func(**kwargs)
                        gold_success_counter.inc()
                        end_logging(meta)
                        return result
                    except Exception as e:
                        gold_failure_counter.inc()
                        end_logging(meta, error=e)
                        raise
                return _callable

            task_callable = create_task_callable(script_path, task_id, stage_params)

            task = PythonOperator(
                task_id=task_id,
                python_callable=task_callable,
                op_kwargs={**stage_params, "execution_date": "{{ ds }}"},
                dag=dag,
            )

            previous_task >> task
            previous_task = task

        return dag


# =========================================================
# Instantiate DAG
# =========================================================
dag = GoldPipelineDAG("breweries_gold_pipeline.yml").build()
