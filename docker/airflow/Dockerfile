# Airflow + the two providers this DAG uses, pinned by Airflow's own constraints file so the
# dependency set is one the Airflow project has tested together.
FROM apache/airflow:2.10.5-python3.11

RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon apache-airflow-providers-snowflake pytest \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
