import json
import logging
import datetime

def log_task_event(task_name: str, event: str, details: dict = None, level: str = "info"):
    """
    Logs structured JSON events for Airflow tasks.
    Each log entry includes timestamp, task_name, event_type, and metadata (details).

    Args:
        task_name (str): Name of the Airflow task.
        event (str): Event description, e.g. "started", "success", "failed".
        details (dict, optional): Additional metadata to include.
        level (str, optional): Logging level ("info" or "error").
    """
    log_data = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "task_name": task_name,
        "event": event,
        "details": details or {},
    }
    log_str = json.dumps(log_data, ensure_ascii=False)

    if level == "error":
        logging.error(log_str)
    else:
        logging.info(log_str)

    print(log_str)  # ensures visibility in Airflow logs as well
    return log_data


def wrap_task_with_logging(task_name, func):
    """
    Wraps any Airflow PythonOperator callable to automatically log start/success/failure
    using `log_task_event` in structured JSON format.

    Args:
        task_name (str): Task ID name.
        func (callable): Original Python callable function.

    Returns:
        callable: Wrapped function ready for Airflow's PythonOperator.
    """
    def _wrapped(**kwargs):
        context = kwargs.get("context", {})
        log_task_event(task_name, "started", {"context": str(context)})

        try:
            result = func(**kwargs)
            log_task_event(task_name, "success", {"result": str(result)})
            return result
        except Exception as e:
            log_task_event(task_name, "failed", {"error": str(e)}, level="error")
            raise

    return _wrapped


def log_external_dependency(sensor_task_id: str, external_dag_id: str, external_task_id: str, state: str):
    """
    Log structured event for cross-DAG dependencies, useful when tracking ExternalTaskSensor.
    """
    log_task_event(
        sensor_task_id,
        "external_dependency",
        {
            "external_dag": external_dag_id,
            "external_task": external_task_id,
            "state": state,
        }
    )
