"""
DAG: yellow_taxi_to_gcs_2024_h1
Run locally with LocalExecutor.  Stops at GCS as required by the assignment.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup

import os


# CONSTANTS
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = [f"{m:02d}" for m in range(1, 7)]             # 01-06
FILES = [f"yellow_tripdata_2024-{m}.parquet" for m in MONTHS]
LOCAL_DIR = Path("/tmp/taxi_2024_h1")
GCP_PROJECT_ID = "dtc-de-course-466908"
GCP_GCS_BUCKET = "dtc-de-course-466908-m03-bucket"

# DAG and default arguments
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yellow_taxi_to_gcs_2024_h1",
    description="Stage Jan-Jun 2024 Yellow Taxi Parquet files to GCS",
    start_date=datetime(2025, 8, 4),
    max_active_runs=1,
) as dag:

    # ------------------------------------------------------------------
    @task
    def download_file(file_name: str) -> str: 
        """Download one Parquet file to LOCAL_DIR; return local path."""
        import requests

        LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        local_path = LOCAL_DIR / file_name
        if local_path.exists():
            # Avoid re-downloading on retries / re-runs
            return str(local_path)

        url = f"{BASE_URL}/{file_name}"
        print(f"Downloading {url}")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        print(f"Saved {local_path}")
        return str(local_path)

    # ------------------------------------------------------------------
    @task
    def upload_to_gcs(source_path: str, bucket_name:str, 
                      file_name: str):
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=file_name,
            filename=source_path,
            mime_type="application/octet-stream",
        )
        print(f"Uploaded {file_name} to GCS bucket {bucket_name}.")
    
    # ------------------------------------------------------------------
    @task
    def verify_gcs(file_name: str, bucket_name: str) -> None:
        """Verify file exists in GCS."""
        gcs_hook = GCSHook()
        if not gcs_hook.exists(bucket_name, file_name):
            raise FileNotFoundError(f"File {file_name} not found in GCS bucket {bucket_name}.")
        print(f"Verified {file_name} exists in GCS bucket {bucket_name}.")

    # ------------------------------------------------------------------
    for month, file_name in zip(MONTHS, FILES):
        clean_name = file_name.replace(".parquet", "") 
        with TaskGroup(group_id=f"process_month_{month}") as tg:
            download_task = download_file.override(
                task_id=f"download_{file_name}"
            )(file_name)

            upload_task = upload_to_gcs.override(
                task_id=f"upload_{file_name}"
            )(download_task, GCP_GCS_BUCKET, file_name)

            verify_task = verify_gcs.override(
                task_id=f"verify_{file_name}"
            )(file_name, GCP_GCS_BUCKET)

            # Set dependencies
            download_task >> upload_task >> verify_task
