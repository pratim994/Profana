
from datetime import datetime
import uuid
import json
import time
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airscholar",
    "start_date": datetime(2026, 10, 30),
}


def get_data():
    import requests

    response = requests.get(
        "https://randomuser.me/api/",
        timeout=15
    )
    response.raise_for_status()

    return response.json()["results"][0]


def format_data(res):
    location = res["location"]
    street = location["street"]

    return {
        "id": str(uuid.uuid4()),
        "first_name": res["name"]["first"],
        "last_name": res["name"]["last"],
        "gender": res["gender"],
        "address": (
            f"{street['number']} {street['name']}, "
            f"{location['city']}, "
            f"{location['state']}, "
            f"{location['country']}"
        ),
        "post_code": str(location["postcode"]),
        "email": res["email"],
        "username": res["login"]["username"],
        "dob": res["dob"]["date"],
        "registered_date": res["registered"]["date"],
        "phone": res["phone"],
        "picture": res["picture"]["medium"],
    }


def stream_data():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["broker:29092"],
        max_block_ms=5000,
        request_timeout_ms=10000,
    )

    deadline = time.monotonic() + 60

    try:
        while time.monotonic() < deadline:
            try:
                user = format_data(get_data())

                future = producer.send(
                    "users_created",
                    json.dumps(user).encode("utf-8"),
                )

                future.get(timeout=10)

                logging.info(
                    "Published user: %s %s",
                    user["first_name"],
                    user["last_name"],
                )

            except Exception:
                logging.exception("Failed to fetch or publish user")
                time.sleep(2)

    finally:
        producer.flush()
        producer.close()


with DAG(
    dag_id="user_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["kafka", "streaming"],
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
    )
