FROM apache/airflow:2.9.1-python3.12

# ============================================================
# Instala pacotes de sistema necessários (Java, Git, Curl, Spark)
# ============================================================
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        git \
        curl \
        procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ============================================================
# Variáveis de ambiente
# ============================================================
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/opt/airflow:/opt/airflow/src:/opt/airflow/src/libs:/opt/airflow/src/pipelines:/opt/airflow/dags"

# ============================================================
# Define o diretório de trabalho
# ============================================================
WORKDIR /opt/airflow

# ============================================================
# Copia os arquivos para dentro do container
# ============================================================
COPY dags/ dags/
COPY src/ src/
COPY alerts/ alerts/
COPY configs/ configs/
COPY requirements.txt ./
COPY entrypoint.sh /entrypoint.sh

# ============================================================
# Instala as dependências Python
# ============================================================
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# ============================================================
# Permissões e criação de diretórios
# ============================================================
USER root
RUN chmod +x /entrypoint.sh && \
    mkdir -p /opt/airflow/data/{bronze,silver,gold} /opt/airflow/logs /opt/airflow/mlruns && \
    chown -R airflow:root /opt/airflow && \
    chmod -R 775 /opt/airflow

# ============================================================
# Finaliza como usuário airflow e expõe a porta
# ============================================================
USER airflow
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
