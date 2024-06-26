FROM apache/airflow:2.9.1

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" && \
    pip install --no-cache-dir -r /requirements.txt
