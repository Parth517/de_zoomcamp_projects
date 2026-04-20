resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }
}

resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id = var.bq_dataset_bronze
  project    = var.project
  location   = var.location
}

resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id = var.bq_dataset_silver
  project    = var.project
  location   = var.location
}

resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id = var.bq_dataset_gold
  project    = var.project
  location   = var.location
}
