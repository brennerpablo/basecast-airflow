# basecast's Airflow image: the official image plus the pipeline libraries it lacks. Versions are pinned
# here, not in .env; the official constraints file keeps the install consistent with Airflow's own pins.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
