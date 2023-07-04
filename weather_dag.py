from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius

def transform_load_data(task_instance):
        data = task_instance.xcom_pull(task_ids="extract_weather_data")
        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_celsius = kelvin_to_celsius(data["main"]["temp"])
        feels_like_celsius= kelvin_to_celsius(data["main"]["feels_like"])
        min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
        max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celsius,
                        "Feels Like (C)": feels_like_celsius,
                        "Minimun Temp (C)":min_temp_celsius,
                        "Maximum Temp (C)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }

        transformed_data_list = [transformed_data]
        df = pd.DataFrame(transformed_data_list)        

        aws_credentials = {"key": "ASIATTWQXH4VLPTUJB3R", "secret": "daTW6FPXquHWz1KQ510FfEU8KLJx2ga0Aol8CuAr", "token": "FwoGZXIvYXdzELf//////////wEaDESLSNDQ5No7oBAGryJqZTB8TT2IjG+DNlebKSi08d1nhymizu41dz2Ly3pPY0WbOyQBi2toxX7KrtxVt+ZKngnx1W4+i27ErtmqDX4U/GZs6X4W8djvfBLeDJOSCpkNyaAI5x4H0jxJA6Cck7uisq2r3OPxg4JjAyjxp5KlBjIo4c19bf5WdP+TdbJH/8H8sUbeanUod4UQxcYDqf2m3CZQRZxLQakFqw=="}

        now = datetime.now()
        dt_string = now.strftime("%d%m%Y%H%M%S")
        dt_string = 'current_weather_data_helsinki_' + dt_string
        df.to_csv(f"s3://open-weather-helsinki/{dt_string}.csv", index=False, storage_options=aws_credentials)





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}



with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily', # or schedule_interval=timedelta(days=1),
        catchup=False) as dag:


# HttpSensor is a sensor that helps to wait for a particular 
# condition/task is met before going to the next task 
# endpoint is the built-in API search request
# http_conn_id is the name to connect to airflow

        is_open_weather_api_ready = HttpSensor(
        task_id ='is_open_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=helsinki&appid=8841e9a4afcb321d9ac47e08278a078c'
        )


        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=helsinki&appid=8841e9a4afcb321d9ac47e08278a078c',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )


        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )


        is_open_weather_api_ready >> extract_weather_data >>  transform_load_weather_data