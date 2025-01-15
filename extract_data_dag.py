import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import os
import json
from transform_function import kelvin_to_fahrenheit, transform_load_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def default_args():
    """
    Default arguments for the DAG
    """
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2023, 1, 1),
        "email": ["myemail@domain.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }

# Define the DAG
with DAG(
    "weather_dag",
    default_args=default_args(),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    # Check if API is available
    is_weather_api_available = HttpSensor(
        task_id="is_weather_api_available",
        endpoint=f"data/2.5/weather?q=Stockholm&appid={os.getenv('API_KEY')}",
        http_conn_id="weather_map_api",
    )

    # Extract weather data
    extract_weather_data = SimpleHttpOperator(
        task_id="extract_weather_data",
        http_conn_id="weather_map_api",
        endpoint=f"data/2.5/weather?q=Stockholm&appid={os.getenv('API_KEY')}",
        method="GET",
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )

    # Transform and load weather data
    transform_load_weather_data = PythonOperator(
        task_id="transform_load_weather_data",
        python_callable=transform_load_data,
    )

    is_weather_api_available >> extract_weather_data >> transform_load_weather_data
