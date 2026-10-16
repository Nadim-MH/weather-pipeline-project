from datetime import datetime, timedelta
import json
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash import BashOperator
from saudi_cities import SAUDI_CITIES

BUCKET_NAME = "your-gcs-bucket-name"
PROJECT_ID = "your-gcp-project-id"
ARCHIVE_LAG_DAYS = 6

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="bronze_weather_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 29, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "transform", "weather"]
)
def weather_pipeline():

    @task
    def fetch_weather(city: dict, **context) -> dict:
        logical_date = context["ds"]
        request_date = (datetime.strptime(logical_date, "%Y-%m-%d") - timedelta(days=ARCHIVE_LAG_DAYS)).strftime("%Y-%m-%d")
        
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "start_date": request_date,
            "end_date": request_date,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
            "temperature_unit": "celsius",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
            "timezone": "UTC"
        }
        
        response = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params=params,
            timeout=60
        )
        response.raise_for_status()
        
        return {
            "city_id": city["city_id"],
            "city_name": city["city_name"],
            "request_date": request_date,
            "pulled_at": context["ts"],
            "run_stamp": context["ts_nodash"],
            "weather_data": response.json()
        }

    @task
    def upload_to_gcs(payload: dict) -> dict:
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        file_path = f"raw_weather/city_id={payload['city_id']}/request_date={payload['request_date']}/weather_{payload['run_stamp']}.json"
        
        gcs_hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=file_path,
            data=json.dumps(payload),
            mime_type="application/json"
        )
        print(f"Uploaded {payload['city_name']} ({payload['request_date']}) to {file_path}")
        
        return payload

    @task
    def load_to_bq_bronze(payload: dict):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery
        
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        client = bq_hook.get_client()
        
        raw_api_data = json.dumps(payload['weather_data'])
        
        merge_query = f"""
            MERGE `{PROJECT_ID}.weather_bronze.raw_weather_responses` T
            USING (
                SELECT 
                    @city_id AS city_id,
                    CAST(@request_date AS DATE) AS request_date,
                    CAST(@pulled_at AS TIMESTAMP) AS pulled_at,
                    'open-meteo' AS source,
                    PARSE_JSON(@raw_payload, wide_number_mode => 'round') AS payload
            ) S
            ON T.city_id = S.city_id AND T.request_date = S.request_date
            WHEN MATCHED THEN
                UPDATE SET pulled_at = S.pulled_at, payload = S.payload
            WHEN NOT MATCHED THEN
                INSERT (city_id, request_date, pulled_at, source, payload)
                VALUES (S.city_id, S.request_date, S.pulled_at, S.source, S.payload)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city_id", "INT64", payload['city_id']),
                bigquery.ScalarQueryParameter("request_date", "STRING", payload['request_date']),
                bigquery.ScalarQueryParameter("pulled_at", "STRING", payload['pulled_at']),
                bigquery.ScalarQueryParameter("raw_payload", "STRING", raw_api_data),
            ]
        )
        
        client.query(merge_query, job_config=job_config).result()
        print(f"Merged {payload['city_name']} data into BigQuery Bronze table.")

    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /workspaces/weather-pipeline-project/weather_transform && dbt build",
    )

    # Map tasks and define dependencies
    fetched_data = fetch_weather.expand(city=SAUDI_CITIES)
    uploaded_data = upload_to_gcs.expand(payload=fetched_data)
    bronze_tasks = load_to_bq_bronze.expand(payload=uploaded_data)

    # Execute dbt build after all 14 Bronze tasks complete
    bronze_tasks >> run_dbt_transformations

weather_pipeline()
