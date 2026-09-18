FROM apache/airflow:3.0.6

USER root

# Docker CLI lets Airflow launch the dedicated UrbanPulse
# Spark execution containers through the host Docker daemon.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker

WORKDIR /opt/airflow