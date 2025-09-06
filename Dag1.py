import os
from datetime import datetime, timezone, timedelta
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
from io import StringIO

GCP_CONN_ID= "google_cloud_connection_2"
gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

# Retrieve required names
from requirement.main_requirements import get_project, get_input_bucket, get_input_directory, get_output_landing_directory, get_moving_directory
PROJECT_ID = get_project()
INPUT_BUCKET= get_input_bucket()
INPUT_DIRECTORY = get_input_directory()
OUTPUT_LANDING_DIRECTORY = get_output_landing_directory()
OUTPUT_MOVING_DIRECTORY = get_moving_directory()

def list_gcs_files():

    #FOR COMPOSER DAG
    # client = storage.Client()
    # bucket = client.get_bucket(INPUT_BUCKET)
    # blobs = bucket.list_blobs(prefix= directory_path)
    # file_paths = [blob.name for blob in blobs if not blob.name.endswith('/')]

    file_paths = gcs_hook.list(bucket_name=INPUT_BUCKET, prefix=INPUT_DIRECTORY)
    return file_paths

def move_gcs_file(bucket_name, source_path, destination_path):

    gcs_hook.copy(
        source_bucket=bucket_name,
        source_object=source_path,
        destination_bucket=bucket_name,
        destination_object=destination_path
    )

    # Delete original file
    gcs_hook.delete(bucket_name=bucket_name, object_name=source_path)
    print(f"Moved file from gs://{bucket_name}/{source_path} → gs://{bucket_name}/{destination_path}")

def func_files_processing():
    #gs://sanjeev-project-landing/NSE_stocks_5_min_sample/ABB_5minute.csv

    all_files = list_gcs_files()
    for file_path in all_files:
        file_name = file_path.split("/")[-1]
        print("file_path:", file_path)
        print("file_name:", file_name)

        file_content_bytes = gcs_hook.download(bucket_name=INPUT_BUCKET, object_name=file_path)
        file_content_str = file_content_bytes.decode("utf-8").split("\n")[1:]
        current_timestamp = datetime.now(timezone.utc).isoformat()

        list_file = list()
        for line in file_content_str:
            try:
                list_line = line.split(",")
                if len(list_line) == 1:
                    continue
                dict_line = dict()
                dict_line["datetime"] = datetime.strptime(list_line[0], "%Y-%m-%d %H:%M:%S").isoformat()
                dict_line["open"] = float(list_line[1])
                dict_line["high"] = float(list_line[2])
                dict_line["low"] = float(list_line[3])
                dict_line["close"] = float(list_line[4])
                dict_line["volume"] = int(list_line[5])
                dict_line["filename"] = file_path
                dict_line["rec_cre_tms"] = current_timestamp
                dict_line["rec_chng_tms"] = current_timestamp
                dict_line["stock_symbol"] = file_name.split("_")[0]
                # date,open,high,low,close,volume
                list_file.append(dict_line)
            except:
                print(list_line)
                continue

        # convert to CSV string
        df = pd.DataFrame(list_file)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        output_gcs_path = OUTPUT_LANDING_DIRECTORY + file_name

        #upload to GCS
        gcs_hook.upload(
            bucket_name=INPUT_BUCKET,
            object_name=output_gcs_path,
            data=csv_content,
            mime_type="text/csv"
        )
        print("file", file_name, "loaded successfully")

        #transfer the file from one original location to landing directory
        destination_path = OUTPUT_MOVING_DIRECTORY + file_name
        move_gcs_file(INPUT_BUCKET, file_path, destination_path)

default_args = {
    # "start_date": yesterday,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes= 5)
}

with DAG(
    dag_id = "DAG1",
    catchup= False,
    schedule_interval = None, #timedelta(days =1),
    default_args= default_args
) as dag:

    start = EmptyOperator(
        task_id="start_nse_5_min_dag1",
        dag = dag
    )

    file_process = PythonOperator(
        task_id = "process_files",
        python_callable= func_files_processing
    )

    end = EmptyOperator(
        task_id="end_nse_5_min_dag1",
        dag=dag
    )

start >> file_process >> end
