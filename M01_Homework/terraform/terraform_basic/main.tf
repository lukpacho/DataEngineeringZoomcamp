terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.45.0"
    }
  }
}

provider "google" {
  credentials = file("./keys/my-creds.json")
  project     = "dtc-de-course-466908"
  region      = "europe-west10"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name     = "my_data_lake_bucket"
  location = "EU"

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "my_bigquery_dataset"
  project    = "dtc-de-course-466908"
  location   = "EU"
}
