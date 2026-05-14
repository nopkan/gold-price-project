ARG AIRFLOW_VERSION=3.2.0

FROM apache/airflow:${AIRFLOW_VERSION}

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt
