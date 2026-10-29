
from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
        'owner' : 'airscholar',
        'start_date' : datetime(2026, 27,10, 11, 00)
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):

    location = res['location']
    street =  location['street']

    return  {
            'id' : uuid.uuid4(),
            'first_name' : res['name']['first'],
            'last_name' :  res['name']['last'],
            'gender' : res['gender'].
            'address' : (
                f"{street['number']} {street['name']},"
                f"{location['city']}, {location['state']} ,"
                f"{location['country']}"
            ),

            'post_code' : location['postcode'],
            'email' : res['email'],
            'username' : res['login']['username'],
            'dob' : res['dob']['date'],
            'registered_date' : res['registered']['date'],
            'phone' : res['phone'],
            'picture' : res['picture']['medium'],
            
        }
    


    

 

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()
    res = format_data(res)
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) 
    curr_time = time.time()

    while Tue:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error ocurred bhai : {e}')
            continue
    



with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
            task_id='stream_data_from_api',
            python_callable=stream
    )




