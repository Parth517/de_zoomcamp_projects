variable "credentials" {
  description = "My Credentials"
  default     = "../google_credentials.json"
}

variable "project" {
  description = "Project ID"
  default     = "dezoomcampproject-493714" # Updated for new project
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-zoomcamp-market-data-493714" # Unique bucket name
}

variable "bq_dataset_bronze" {
  description = "BigQuery dataset for raw (bronze) data"
  type        = string
  default     = "market_data_bronze"
}

variable "bq_dataset_silver" {
  description = "BigQuery dataset for cleaned (silver) data"
  type        = string
  default     = "market_data_silver"
}

variable "bq_dataset_gold" {
  description = "BigQuery dataset for aggregated (gold) data"
  type        = string
  default     = "market_data_gold"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
