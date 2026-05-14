ARG AIRFLOW_VERSION=3.2.0

FROM apache/airflow:${AIRFLOW_VERSION}

COPY requirements.txt /tmp/requirements.txt
COPY --chown=airflow:root config/simple_auth_manager_passwords.json /opt/airflow/config/simple_auth_manager_passwords.json

RUN pip install --no-cache-dir -r /tmp/requirements.txt
