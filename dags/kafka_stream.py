import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Fetch data from the Go API endpoint
def get_data():
    res = requests.get("http://localhost:8085/data")  # Your Go API endpoint
    if res.status_code == 200:
        return res.json()  # Return the list of data from the API
    else:
        raise Exception(f"Failed to fetch data from the API. Status code: {res.status_code}")

# Format the fetched data to include lesson_id, user_id, and geolocation
def format_data(res):
    formatted_data = []
    for item in res:
        data = {}
        # Assuming each item in res contains 'name', 'value', and other necessary information
        data['lesson_id'] = uuid.uuid4()  # Generate a unique lesson_id
        data['user_id'] = uuid.uuid4()    # Generate a unique user_id
        data['geolocation'] = item.get('geolocation', 'Unknown location')  # Extract geolocation or use a default
        
        # Add other fields if necessary
        formatted_data.append(data)

    return formatted_data

# Ensure UUID is serializable
def uuid_default(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)  # Convert UUID to string
    raise TypeError(f"Object of type {obj.__class__.__name__} is not serializable")

# Stream the formatted data to a Kafka topic
def stream_data():
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:
            res = get_data()  # Fetch the data from the Go API
            formatted_data = format_data(res)  # Format it to include lesson_id, user_id, and geolocation
            for data in formatted_data:
                json_data = json.dumps(data, default=uuid_default)  # Serialize UUIDs

                # Send the data to Kafka
                producer.send('users_created', json_data.encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

# Define the DAG
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
