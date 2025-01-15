import logging
import pandas as pd
import os
from datetime import datetime

def kelvin_to_fahrenheit(temp_in_kelvin):
    """
    Convert temperature from Kelvin to Fahrenheit.
    Args:
        temp_in_kelvin (float): Temperature in Kelvin.
    Returns:
        float: Temperature in Fahrenheit.
    """
    if temp_in_kelvin is None:
        return None
    return (temp_in_kelvin - 273.15) * (9 / 5) + 32

def transform_load_data(task_instance):
    """
    Transform and load weather data.
    Args:
        task_instance (TaskInstance): The task instance from Airflow for pulling XCom data.
    """
    try:
        logging.info("Pulling data from XCom.")
        data = task_instance.xcom_pull(task_ids="extract_weather_data")
        if not data or "name" not in data:
            raise ValueError("Invalid or missing data from XCom")

        logging.info("Transforming data.")
        city = data["name"]
        weather_description = data["weather"][0]["description"]
        temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
        sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
        sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

        transformed_data = {
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_fahrenheit,
            "Feels Like (F)": feels_like_fahrenheit,
            "Minimum Temp (F)": min_temp_fahrenheit,
            "Maximum Temp (F)": max_temp_fahrenheit,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time,
        }

        logging.info("Creating DataFrame.")
        df_weather = pd.DataFrame([transformed_data])

        aws_bucket = os.getenv("AWS_BUCKET_NAME")
        aws_access_key = os.getenv("AWS_ACCESS_KEY")
        aws_secret_key = os.getenv("AWS_SECRET_KEY")

        if not aws_bucket or not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials or bucket name not set in environment variables.")

        dt_string = datetime.now().strftime("%d%m%Y%H%M%S")
        filename = f"current_weather_data_{city}_{dt_string}.csv"

        logging.info(f"Saving DataFrame to S3 bucket: {aws_bucket}.")
        df_weather.to_csv(f"s3://{aws_bucket}/{filename}", index=False)
        logging.info("Data successfully saved to S3.")

    except Exception as e:
        logging.error(f"Error in transform_load_data: {e}")
        raise
