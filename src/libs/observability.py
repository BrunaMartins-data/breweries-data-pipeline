import mlflow
import logging
from datetime import datetime
from typing import Dict, Any, Optional

RESERVED_KEYS = {"status", "stage", "start_time"}


def init_logging(stage_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Initialize MLflow and structured logging for a pipeline stage.
    Starts a new MLflow run and logs parameters and tags.
    """
    logging.info(f"Starting stage: {stage_name}")
    start_time = datetime.utcnow()

    mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
    mlflow.set_experiment("airflow_pipeline_logs")

    if mlflow.active_run():
        mlflow.end_run()

    run = mlflow.start_run(run_name=stage_name)
    run_id = run.info.run_id

    # Log parameters (pula chaves reservadas para evitar sobrescrita)
    if params:
        for key, value in params.items():
            if key in RESERVED_KEYS:
                continue
            try:
                mlflow.set_tag(key, value)
            except Exception:
                logging.debug(f"Could not log tag: {key}")

    mlflow.set_tag("stage", stage_name)
    mlflow.set_tag("status", "RUNNING")
    mlflow.set_tag("start_time", str(start_time))

    return {"stage": stage_name, "start_time": start_time, "run_id": run_id}


def end_logging(meta: Dict[str, Any], df: Any = None, data: Any = None, error: Optional[Exception] = None) -> None:
    """
    Finalize MLflow logging for a stage.  
    Logs metrics, success/failure, and closes the MLflow run.
    """
    run_id = meta.get("run_id")
    duration = (datetime.utcnow() - meta["start_time"]).total_seconds()

    mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
    mlflow.set_experiment("airflow_pipeline_logs")

    # Garante que estamos no run correto
    if not mlflow.active_run() or mlflow.active_run().info.run_id != run_id:
        try:
            mlflow.start_run(run_id=run_id)
        except Exception:
            pass

    try:
        if error:
            logging.error(f"Stage {meta['stage']} failed: {error}")
            mlflow.set_tag("status", "FAILED")
            mlflow.set_tag("error_message", str(error))
        else:
            logging.info(f"Stage {meta['stage']} completed successfully in {duration:.2f}s.")
            mlflow.set_tag("status", "SUCCESS")

            # Loga quantidade de linhas do DataFrame, se aplicável
            if df is not None and hasattr(df, "count"):
                try:
                    mlflow.log_metric("rowcount", float(df.count()))
                except Exception:
                    pass

            # Loga os primeiros campos do dicionário de saída
            if isinstance(data, dict):
                try:
                    summary = {k: data[k] for k in list(data)[:5]}
                    mlflow.set_tag("output_summary", str(summary))
                except Exception:
                    pass

        mlflow.log_metric("duration_sec", float(duration))

    finally:
        if mlflow.active_run():
            mlflow.end_run()
