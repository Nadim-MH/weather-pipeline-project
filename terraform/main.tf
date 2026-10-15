terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==========================================
# 0. Enable required GCP APIs
# ==========================================
resource "google_project_service" "required_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ==========================================
# 1. Service Account (used by Airflow + dbt)
# ==========================================
resource "google_service_account" "pipeline_sa" {
  account_id   = "weather-pipeline-airflow-dbt"
  display_name = "Weather Pipeline Service Account (Airflow + dbt)"

  depends_on = [google_project_service.required_apis]
}

resource "google_project_iam_member" "sa_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ==========================================
# 2. Bronze Layer (Data Lake) - GCS Bucket
# ==========================================
resource "google_storage_bucket" "bronze_data_lake" {
  name                        = "${var.project_id}-bucket-bronze"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true 

  lifecycle_rule {
    condition {
      age = 365 * 5
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required_apis]
}

# ==========================================
# 3. Bronze Layer - BigQuery Dataset (raw table)
# ==========================================
resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id                 = var.bronze_dataset_name
  friendly_name              = "Bronze Layer - Raw Weather Data"
  description                = "Raw API responses loaded as-is, append-only"
  location                   = var.region
  delete_contents_on_destroy = true

  depends_on = [google_project_service.required_apis]
}

# ==========================================
# 4. Silver Layer - BigQuery Dataset
# ==========================================
resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id                 = var.silver_dataset_name
  friendly_name              = "Silver Layer - Cleansed Weather Data"
  description                = "Contains stg_weather_hourly and quarantine tables"
  location                   = var.region
  delete_contents_on_destroy = true

  depends_on = [google_project_service.required_apis]
}

# ==========================================
# 5. Gold Layer - BigQuery Dataset
# ==========================================
resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id                 = var.gold_dataset_name
  friendly_name              = "Gold Layer - Star Schema"
  description                = "Contains fct tables, dim tables, and pipeline_runs"
  location                   = var.region
  delete_contents_on_destroy = true

  depends_on = [google_project_service.required_apis]
}