# docker/Dockerfile
# PayPal Pipeline Dockerfile

FROM apache/airflow:2.8.1-python3.11

USER airflow
COPY --chown=airflow:root scripts/ /app/scripts/
COPY --chown=airflow:root config/ /app/config/
RUN mkdir -p /app/data

WORKDIR /opt/airflow