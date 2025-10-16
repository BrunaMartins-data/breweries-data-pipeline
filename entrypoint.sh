#!/usr/bin/env bash
set -euo pipefail

# 1) Se recebeu comandos (ex: "airflow db migrate"), executa e sai
if [[ $# -gt 0 ]]; then
  exec "$@"
fi

# 2) Ambiente
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
export PYTHONPATH="${PYTHONPATH:-}:${AIRFLOW_HOME}:${AIRFLOW_HOME}/src:${AIRFLOW_HOME}/src/libs:${AIRFLOW_HOME}/src/pipelines:${AIRFLOW_HOME}/dags"

echo "Starting Airflow entrypoint..."
echo "PYTHONPATH: $PYTHONPATH"
echo "AIRFLOW_HOME: $AIRFLOW_HOME"

# 3) Aguarda Postgres
echo "Waiting for Postgres..."
until pg_isready -h postgres -U airflow -d airflow >/dev/null 2>&1; do
  sleep 2
done
echo " Postgres is ready."

# 4) Garante conexão correta no runtime (sem mexer em airflow.cfg na mão)
echo " Checking Airflow metadata..."
airflow info | grep -E "sql_alchemy_conn|airflow_home" || true

# 5) MIGRATE SEMPRE (substitui init e evita estado parcial)
echo " Migrating Airflow metadata (db migrate)..."
airflow db migrate  # idempotente: se já estiver migrado, não faz nada

# 6) (Opcional) Checagem rápida de schema
airflow db check || true

# 7) Cria usuário admin se não existir
echo " Ensuring admin user..."
if ! airflow users list 2>/dev/null | grep -q "^ *airflow *|"; then
  airflow users create \
    --username "${AIRFLOW_USER:-airflow}" \
    --password "${AIRFLOW_PASSWORD:-airflow}" \
    --firstname "${AIRFLOW_FIRSTNAME:-Airflow}" \
    --lastname  "${AIRFLOW_LASTNAME:-Admin}" \
    --role Admin \
    --email "${AIRFLOW_EMAIL:-airflow@example.com}"
else
  echo " Admin user already exists."
fi

# 8) Sobe webserver + scheduler (em background e foreground)
echo " Starting Airflow webserver..."
airflow webserver --port 8080 &

echo " Starting Airflow scheduler..."
exec airflow scheduler
