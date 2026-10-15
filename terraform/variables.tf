variable "project_id" {
  description = "The GCP Project ID"
  type        = string
  # بدون default عمداً
}

variable "region" {
  description = "The region for GCS and BigQuery"
  type        = string
  default     = "europe-west3"
}

variable "bronze_dataset_name" {
  description = "Name of the Bronze BigQuery Dataset"
  type        = string
  default     = "weather_bronze"
}

variable "silver_dataset_name" {
  description = "Name of the Silver BigQuery Dataset"
  type        = string
  default     = "weather_silver"
}

variable "gold_dataset_name" {
  description = "Name of the Gold BigQuery Dataset"
  type        = string
  default     = "weather_gold"
}