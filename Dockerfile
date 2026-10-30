
FROM apache/airflow:2.6.0-python3.9

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    requests==2.31.0 \
    kafka-python==2.0.2
