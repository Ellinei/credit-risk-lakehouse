# Airflow 3 image with the AWS and Databricks providers, plus duckdb for the replayer.
# Packages are baked in at build time instead of _PIP_ADDITIONAL_REQUIREMENTS, so
# containers start fast and every restart uses the same versions.
FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt" \
      -r /requirements.txt
