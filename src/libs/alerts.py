import logging
import requests
from typing import Any, Dict
from airflow.models import TaskInstance
from airflow.utils.email import send_email

try:
    from prometheus_client import Counter
    PROMETHEUS_ENABLED = True
except ImportError:
    PROMETHEUS_ENABLED = False


# Prometheus counters (optional)
if PROMETHEUS_ENABLED:
    airflow_failures = Counter(
        "airflow_task_failures_total",
        "Total number of failed Airflow tasks",
        ["dag_id", "task_id"],
    )
    airflow_successes = Counter(
        "airflow_task_success_total",
        "Total number of successful Airflow tasks",
        ["dag_id", "task_id"],
    )
else:
    airflow_failures = airflow_successes = None


def notify_failure(context: Dict[str, Any]) -> None:
    """
    Generic failure callback for Airflow tasks.
    Logs failure details, increments Prometheus counter, and sends an email + alert.
    """
    ti: TaskInstance = context["task_instance"]
    dag_id, task_id = ti.dag_id, ti.task_id
    execution_date = str(context.get("execution_date"))
    log_url = getattr(ti, "log_url", "Unavailable")

    logging.error(f"Task failed: {dag_id}.{task_id} at {execution_date}")

    # Prometheus metric
    if airflow_failures:
        airflow_failures.labels(dag_id=dag_id, task_id=task_id).inc()

    # Email notification
    subject = f"[Airflow Alert] Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3>Airflow Task Failed</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Log:</b> <a href="{log_url}">View Log</a></p>
    """

    try:
        send_email(
            to=["brunatavares81@gmail.com"],
            subject=subject,
            html_content=html_content,
        )
        logging.info("Failure notification email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")

    # Alertmanager notification (optional)
    try:
        requests.post(
            "http://alertmanager:9093/api/v1/alerts",
            json=[
                {
                    "labels": {
                        "alertname": "AirflowTaskFailure",
                        "dag": dag_id,
                        "task": task_id,
                    },
                    "annotations": {
                        "description": f"Task {task_id} from DAG {dag_id} failed.",
                    },
                    "generatorURL": log_url,
                }
            ],
            timeout=5,
        )
        logging.info("📡 Alert sent to Alertmanager.")
    except Exception as e:
        logging.warning(f"Could not send alert to Alertmanager: {e}")


def notify_success(context: Dict[str, Any]) -> None:
    """
    Optional success callback for Prometheus metrics.
    Increments success counter when a task completes successfully.
    """
    if not airflow_successes:
        return

    ti: TaskInstance = context["task_instance"]
    airflow_successes.labels(dag_id=ti.dag_id, task_id=ti.task_id).inc()
    logging.info(f"Task succeeded: {ti.dag_id}.{ti.task_id}")
