# 🌤️ Saudi Arabia Weather Analytics & Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE.svg?logo=Apache%20Airflow&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-4285F4.svg?logo=google-cloud&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B.svg?logo=dbt&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC.svg?logo=terraform&logoColor=white)
![Looker Studio](https://img.shields.io/badge/Looker%20Studio-BI-4285F4.svg?logo=google&logoColor=white)

## 📌 Project Overview & Problem Statement
Analyzing climate patterns across Saudi Arabia requires reliable, historical, and daily-updated meteorological data spanning diverse geographical regions. Raw weather APIs return deeply nested JSON structures, are prone to network timeouts during batch extraction, and lack built-in data quality guarantees.

This project implements an end-to-end, production-grade **ELT Data Engineering Pipeline** that extracts historical (2021–Present) and daily weather observations for **14 major Saudi cities**[cite: 7]. Using the **Medallion Architecture (Bronze, Silver, Gold)** on Google Cloud Platform (BigQuery)[cite: 12], the pipeline automates ingestion via **Apache Airflow**[cite: 12], enforces strict data contracts, deduplication, and anomaly quarantining via **dbt**[cite: 4, 12], provisions cloud infrastructure via **Terraform**[cite: 12], and delivers actionable climate and pipeline-observability insights through a 3-page **Looker Studio** dashboard[cite: 7, 8, 9, 12].

🔗 **[Explore the Live Looker Studio Dashboard](https://datastudio.google.com/reporting/d8a1eccb-1025-4494-9730-15dbc2ff7eb2)**

---

## 🏗️ Architecture & Data Flow

> ![Pipeline Architecture](docs/images/architecture.png) *(Replace with exported image from `de_weather_project.drawio.pdf`)*[cite: 12]

```text
[Open-Meteo API] 
       │
       ▼ (Python Extraction: One-Time Historical Backfill & Daily Batch)
[Apache Airflow] ──► [BigQuery: Bronze Layer] (`raw_weather_responses` - Append-Only JSON)
                              │
                              ▼ (dbt Incremental Merge, UNNEST, Deduplication & Quarantine)
                     [BigQuery: Silver Layer] (`stg_weather_hourly`, `int_weather_hourly_clean`, `quarantine_weather_hourly`)
                              │
                              ▼ (dbt Star Schema, Aggregations & Observability Marts)
                     [BigQuery: Gold Layer]   (`fct_daily_weather`, `fct_weather_hourly`, `mart_pipeline_health`, `vw_dashboard_daily`)
                              │
                              ▼
                     [Looker Studio BI Dashboard]
```

### Tech Stack
* **Infrastructure as Code (IaC):** Terraform (Automated provisioning of Google BigQuery datasets and cloud resources)[cite: 10, 12].
* **Orchestration & Extraction:** Apache Airflow & Python (Scheduled daily API extraction and historical backfill from the **Open-Meteo API**)[cite: 9, 12].
* **Cloud Data Warehouse:** Google BigQuery[cite: 12].
* **Transformation & Data Quality:** dbt Core (Incremental `merge` models, array unnesting, `dbt_utils` schema tests, and quarantine routing)[cite: 4, 5, 12].
* **Business Intelligence:** Looker Studio[cite: 12].

---

## 🥉🥈🥇 Medallion Data Modeling

### 1. Bronze Layer (Raw Landing)
* **`raw_weather_responses`**: Append-only table storing raw JSON payloads directly from the Open-Meteo API alongside ingestion metadata (`city_id`, `request_date`, `pulled_at`)[cite: 4, 12]. No transformations are applied at this stage to ensure full replayability[cite: 12].

### 2. Silver Layer (Cleansed, Standardized & Quarantined)
* **`stg_weather_hourly`**: 
  * Configured as an `incremental` model with `incremental_strategy='merge'` on a composite `unique_key=['city_id', 'observed_at_utc']`[cite: 4].
  * Optimizes JSON parsing by extracting hourly arrays (`time`, `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `weather_code`) once and flattening them via `UNNEST WITH OFFSET`[cite: 4].
  * Enforces **intra-batch deduplication** using `QUALIFY ROW_NUMBER()`[cite: 4].
* **`int_weather_hourly_clean` & `quarantine_weather_hourly`**: Evaluates domain-specific data quality rules[cite: 5, 12]. Valid observations flow downstream, while corrupted or out-of-bounds records are isolated in the quarantine table for auditing rather than failing silently[cite: 12].

### 3. Gold Layer (Business-Ready Star Schema & Observability)
* **`fct_weather_hourly` & `fct_daily_weather`**: Core fact tables joined with dimensional models (`dim_city`, `dim_date`) providing daily and hourly aggregations (average/min/max temperatures, daily temperature range, total rainfall, and relative humidity)[cite: 5, 7, 8, 12].
* **`mart_pipeline_health`**: Data observability mart tracking daily `records_ingested` vs. `records_quarantined` per `run_date`[cite: 1, 9].
* **`vw_dashboard_daily`**: Denormalized analytical view powering the BI layer[cite: 5].

> **🌍 Geospatial Note (Dammam & Khobar):** Among the 14 monitored cities, **Dammam** and **Khobar** are geographically adjacent in the Eastern Province with closely proximate coordinates[cite: 7, 8]. While their macro-level weather trends closely align, both cities are tracked independently across the pipeline and dimensional model to preserve city-level granularity and demographic completeness on the dashboard[cite: 7, 8].

---

## 🛠️ Engineering Case Study: Catching & Resolving Intra-Batch Duplication

A core design goal of this project is **Pipeline Observability** and **Idempotency**—ensuring that task failures and retries never corrupt downstream analytics[cite: 9, 12]. During production testing, a real-world data engineering incident occurred, was surfaced by our observability tools, and was permanently resolved:

### 1. The Incident (What Happened?)
The Airflow ingestion DAG is designed to pull 14 records daily (one JSON payload per Saudi city) into the append-only Bronze table (`raw_weather_responses`)[cite: 1, 4, 9]. On **October 8, 2026**, a transient network issue triggered an **Airflow task retry**[cite: 1]. Because the Bronze layer is append-only, one city's payload was ingested twice within the same batch—resulting in **15 ingested records** instead of 14[cite: 1].

### 2. How the Pipeline Detected It
The anomaly was caught immediately across two independent defense layers:
1. **Observability Dashboard (`mart_pipeline_health`):** On Page 3 of the Looker Studio dashboard, the daily ingestion monitor explicitly flagged `records_ingested = 15` on Oct 8, 2026 (deviating from the expected baseline of `14`), exposing the duplicate pull in the Bronze layer[cite: 1, 9].
2. **Automated dbt Testing (`dbt build`):** When `stg_weather_hourly` unnested the duplicate city payload, it generated **24 duplicate hourly rows** (1 day × 24 hours) with identical `(city_id, observed_at_utc)` keys[cite: 5]. This immediately tripped the `dbt_utils.unique_combination_of_columns` test (`Got 24 results, configured to fail if != 0`) and prevented corrupted data from reaching the Gold layer[cite: 5].

### 3. Root Cause & Resolution
While `stg_weather_hourly` was configured with `incremental_strategy='merge'` to prevent duplicates *between* historical table data and new runs, BigQuery `MERGE` cannot resolve duplicates that exist *within the incoming source batch itself* (intra-batch duplication)[cite: 4].

To make the Silver staging layer strictly idempotent against Airflow retries, we implemented a window-function deduplication filter directly on the source CTE before array unnesting[cite: 4]:

```sql
WITH source_data AS (
    SELECT 
        city_id,
        request_date,
        pulled_at,
        payload
    FROM {{ source('bronze', 'raw_weather_responses') }}
    
    {% if is_incremental() %}
    WHERE pulled_at > (SELECT MAX(pulled_at) FROM {{ this }})
    {% endif %}

    -- Guarantee intra-batch idempotency by retaining only the latest pull per city/date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY city_id, request_date 
        ORDER BY pulled_at DESC
    ) = 1
)
```
Following this fix, executing `dbt build --select stg_weather_hourly --full-refresh` purged the legacy duplicate rows, passed all 30 model and data tests, and restored the pipeline health table to a clean `14` records per day[cite: 4, 5, 9].

---

## 📊 Dashboard Preview (Looker Studio)

🔗 **[Live Interactive Dashboard](https://datastudio.google.com/reporting/d8a1eccb-1025-4494-9730-15dbc2ff7eb2)**

### Page 1: Overview
Displays macro-level KPIs (`Avg Temperature: 25.86°C`, `Total Rainfall: 11,317 mm`, `Cities Covered: 14`, `Last Pipeline Run`), a geospatial bubble map of Saudi Arabia encoding both rainfall volume (bubble size) and temperature (color gradient), and a multi-year time series of monthly average temperatures across all 5 regions[cite: 7].
> ![Overview Page](docs/images/dashboard_page1_overview.jpg)

### Page 2: City Comparison
Interactive cross-filtering by date range, season, region, and city name, featuring ranked bar charts for Average Temperature (°C), Average Relative Humidity (%), Total Rainfall (mm), and Average Daily Temperature Range (°C)[cite: 8].
> ![City Comparison Page](docs/images/dashboard_page2_cities.jpg)

### Page 3: Seasonal Analysis & Pipeline Health
Breaks down regional rainfall share (Southern region leading at 45.5%), tracks Winter Rainfall across the last 5 winters (`2022–2026`), and features the **Pipeline Health: Daily Ingestion & Quarantine** audit table[cite: 9].
> ![Seasonal & Pipeline Health Page](docs/images/dashboard_page3_health.jpg)

---

## 📂 Repository Structure

```bash
.
├── LICENSE
├── README.md
├── airflow/                        # Airflow configurations & DAG definitions
│   ├── airflow.cfg
│   └── dags/
│       └── weather_pipeline.py
├── keys/                           # GCP Service Account JSON keys (gitignored)
├── terraform/                      # Terraform IaC for GCP BigQuery provisioning
│   ├── main.tf
│   ├── variables.tf
│   └── terraform.tfvars
└── weather_transform/              # dbt Core project
    ├── dbt_project.yml
    ├── packages.yml
    ├── models/
    │   ├── silver/
    │   │   ├── stg_weather_hourly.sql
    │   │   ├── int_weather_hourly_clean.sql
    │   │   ├── quarantine_weather_hourly.sql
    │   │   ├── schema.yml
    │   │   └── sources.yml
    │   └── gold/
    │       ├── fct_weather_hourly.sql
    │       ├── fct_daily_weather.sql
    │       ├── mart_pipeline_health.sql
    │       └── vw_dashboard_daily.sql
    ├── seeds/                      # City coordinates & regional lookup seeds
    └── tests/                      # Custom data tests
```

---

## 🚀 How to Run Locally

### 1. Prerequisites & GCP Credentials
1. Create a Google Cloud Project and a **Service Account** with `BigQuery Admin` (or `BigQuery Data Editor` & `BigQuery Job User`) roles[cite: 11].
2. Download the Service Account JSON key and place it inside the `keys/` directory (e.g., `keys/pipeline-sa.json`)[cite: 10, 11].

---

### 2. Provision Infrastructure (Terraform)
```bash
cd terraform
terraform init
terraform apply
```

---

### 3. Configure & Run dbt Transformations
1. Create a `profiles.yml` file inside `weather_transform/` (or `~/.dbt/profiles.yml`) to connect dbt with BigQuery using your Service Account key[cite: 6, 11]:

```yaml
weather_transform:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: weather_silver
      threads: 4
      keyfile: /full/path/to/weather-pipeline-project/keys/pipeline-sa.json
      location: US # or your specific GCP dataset region (e.g., me-central2)
```

2. Install dbt packages and execute the models and tests[cite: 11]:

```bash
cd weather_transform
dbt deps

# Run full refresh on initial setup or after schema changes
dbt build --full-refresh

# Standard incremental daily build & test execution
dbt build
```

---

### 4. Configure & Start Apache Airflow
1. Export your environment variables and start Airflow from the root project directory[cite: 11]:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/keys/pipeline-sa.json

# Start Airflow services
airflow standalone
```

2. **Configure the `google_cloud_default` Connection in Airflow[cite: 11]:**
   * **Option A (Via CLI - Recommended):**
     ```bash
     airflow connections add 'google_cloud_default' \
         --conn-type 'google_cloud_platform' \
         --conn-extra '{"project": "YOUR_GCP_PROJECT_ID", "key_path": "'$(pwd)'/keys/pipeline-sa.json"}'
     ```
   * **Option B (Via Airflow Web UI):**
     1. Open the Airflow UI (`http://localhost:8080`)[cite: 11].
     2. Navigate to **Admin** ➔ **Connections**[cite: 11].
     3. Edit (or create) **`google_cloud_default`**[cite: 11]:
        * **Connection Type:** `Google Cloud`[cite: 11]
        * **Project ID:** `<YOUR_GCP_PROJECT_ID>`[cite: 11]
        * **Keyfile Path:** `/full/path/to/weather-pipeline-project/keys/pipeline-sa.json`[cite: 10, 11]
