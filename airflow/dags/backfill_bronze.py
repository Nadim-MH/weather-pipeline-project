import io
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import requests
from google.cloud import bigquery, storage
from saudi_cities import SAUDI_CITIES

PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "your-gcs-bucket-name"
CREDENTIALS_FILE = "/path/to/your/service-account-key.json" 
BACKFILL_START_DATE = "2021-01-01"
HOURLY_VARS = "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m"
GCS_UPLOAD_WORKERS = 20
API_FETCH_WORKERS = 4

def get_backfill_end_date(bq_client: bigquery.Client) -> str:
    """Get the day before the oldest date in Bronze to prevent overlaps."""
    query = f"SELECT MIN(request_date) AS min_date FROM `{PROJECT_ID}.weather_bronze.raw_weather_responses`"
    result = list(bq_client.query(query).result())[0]
    if result.min_date is None:
        raise RuntimeError("Bronze table is empty. Run the daily DAG at least once before backfill.")
    return (result.min_date - timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_city_range(city: dict, start_date: str, end_date: str) -> dict:
    """Fetch the full date range for a single city in one API call."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timezone": "UTC"
    }
    
    for attempt in range(3):
        try:
            response = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params=params,
                timeout=120
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {city['city_name']}: {e}")
            time.sleep(10)
    raise RuntimeError(f"Failed to fetch data for {city['city_name']} after 3 attempts.")

def split_into_daily_payloads(full_response: dict) -> list[dict]:
    """Split a multi-year API response into separate daily payloads."""
    hourly = full_response["hourly"]
    times = hourly["time"]
    indices_by_date = defaultdict(list)
    
    for i, ts in enumerate(times):
        indices_by_date[ts[:10]].append(i)
            
    daily_payloads = []
    for date_str, indices in indices_by_date.items():
        if len(indices) != 24:
            continue
            
        daily_hourly = {key: [hourly[key][i] for i in indices] for key in hourly}
        daily_payload = {k: v for k, v in full_response.items() if k != "hourly"}
        daily_payload["hourly"] = daily_hourly
        
        daily_payloads.append({"request_date": date_str, "weather_data": daily_payload})
        
    return daily_payloads

def upload_day_to_gcs(bucket, city: dict, day: dict, pulled_at: str):
    run_stamp = f"{pulled_at.replace(':', '').replace('-', '')}_backfill_{day['request_date']}"
    payload = {
        "city_id": city["city_id"],
        "city_name": city["city_name"],
        "request_date": day["request_date"],
        "pulled_at": pulled_at,
        "run_stamp": run_stamp,
        "weather_data": day["weather_data"]
    }
    
    file_path = f"raw_weather/city_id={city['city_id']}/request_date={day['request_date']}/weather_{run_stamp}.json"
    bucket.blob(file_path).upload_from_string(json.dumps(payload), content_type="application/json")

def main():
    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
    gcs_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = gcs_client.bucket(BUCKET_NAME)
    
    end_date = get_backfill_end_date(bq_client)
    print(f"Backfill Range: From {BACKFILL_START_DATE} to {end_date}")
    
    pulled_at = datetime.now(timezone.utc).isoformat()
    
    all_rows = []
    gcs_tasks = []
    
    # 1. Fetch & Split Data
    with ThreadPoolExecutor(max_workers=API_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_city_range, city, BACKFILL_START_DATE, end_date): city for city in SAUDI_CITIES}
        for future in as_completed(futures):
            city = futures[future]
            try:
                full_response = future.result()
            except Exception as e:
                print(f"Complete failure for {city['city_name']}: {e}")
                continue
                
            daily_payloads = split_into_daily_payloads(full_response)
            print(f"City {city['city_name']} is ready with {len(daily_payloads)} days.")
            
            for day in daily_payloads:
                all_rows.append({
                    "city_id": city["city_id"],
                    "request_date": day["request_date"],
                    "pulled_at": pulled_at,
                    "payload_str": json.dumps(day["weather_data"])
                })
                gcs_tasks.append((city, day))
                
    print(f"\nTotal days processed across all cities: {len(all_rows)}")
    
    # 2. Upload to GCS
    print("\nUploading raw files to GCS...")
    with ThreadPoolExecutor(max_workers=GCS_UPLOAD_WORKERS) as executor:
        futures = [executor.submit(upload_day_to_gcs, bucket, city, day, pulled_at) for city, day in gcs_tasks]
        for i, future in enumerate(as_completed(futures)):
            future.result()
            if (i + 1) % 1000 == 0:
                print(f"Uploaded file {i + 1} / {len(gcs_tasks)}")
    print("GCS upload complete.")
    
    # 3. Load to BigQuery Staging & Merge
    print("\nLoading data into BigQuery staging table...")
    staging_table_id = f"{PROJECT_ID}.weather_bronze._staging_backfill"
    load_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("city_id", "INTEGER"),
            bigquery.SchemaField("request_date", "DATE"),
            bigquery.SchemaField("pulled_at", "TIMESTAMP"),
            bigquery.SchemaField("payload_str", "STRING")
        ],
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    
    ndjson_data = "\n".join(json.dumps(r) for r in all_rows)
    load_job = bq_client.load_table_from_file(
        io.StringIO(ndjson_data), staging_table_id, job_config=load_job_config
    )
    load_job.result()
    print("Staging table load complete.")
    
    print("Executing final MERGE into Bronze table...")
    merge_query = f"""
    MERGE `{PROJECT_ID}.weather_bronze.raw_weather_responses` T
    USING (
        SELECT 
            city_id, 
            request_date, 
            pulled_at, 
            PARSE_JSON(payload_str, wide_number_mode => 'round') AS payload
        FROM `{staging_table_id}`
    ) S
    ON T.city_id = S.city_id AND T.request_date = S.request_date
    WHEN MATCHED THEN
        UPDATE SET pulled_at = S.pulled_at, payload = S.payload
    WHEN NOT MATCHED THEN
        INSERT (city_id, request_date, pulled_at, source, payload)
        VALUES (S.city_id, S.request_date, S.pulled_at, 'open-meteo', S.payload)
    """
    bq_client.query(merge_query).result()
    bq_client.delete_table(staging_table_id, not_found_ok=True)
    
    print(f"\nSUCCESS: Backfill complete. Merged {len(all_rows)} rows into Bronze.")

if __name__ == "__main__":
    main()
