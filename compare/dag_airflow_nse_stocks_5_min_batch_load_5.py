
import os
from datetime import datetime, timezone, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import subprocess

def func_files_processing():
    GCS_URI = "gs://sanjeev-project-landing/NSE_stocks_5_min_sample/*.csv"

    PROJECT_ID = "bq-sessions-sanjeev"
    DATASET_ID = "sanjeev_project_nse_raw_again_5"

    TABLE_ID = "nse_raw_gcs_uri_final"

    schema = [
        bigquery.SchemaField("date", "DATETIME"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER")
    ]

    # Creating a file table first
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_connection_2")
    client = bq_hook.get_client(project_id=PROJECT_ID)
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref = bigquery.TableReference(dataset_ref, TABLE_ID)

    try:
        table = bigquery.Table(table_ref, schema=schema)
        # Add partitioning on "date" column
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        client.create_table(table)

        job_config = bigquery.LoadJobConfig(
            schema= schema,
            source_format = bigquery.SourceFormat.CSV,
            skip_leading_rows = 1,
            autodetect = False,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # source_file = os.path.join(directory_path, file)
        # source_file_bytes = open(source_file, "rb")
        load_job = client.load_table_from_uri(
            GCS_URI,
            table_ref,
            job_config= job_config,
        )
        load_job.result()
        print("successfully inserted")
    except Exception as e:
        print(e)
        print("failed to insert")


#yesterday date value
# yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    # "start_date": yesterday,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes= 5)
}

with DAG(
    dag_id = "Dag_Airflow_3_nse_5_min_shares_batchload_with_tablecreate_gcs_uri",
    catchup= False,
    schedule_interval = None, #timedelta(days =1),
    default_args= default_args
) as dag:

    start = EmptyOperator(
        task_id="start_nse_5_min_dag",
        dag = dag
    )

    file_process = PythonOperator(
        task_id = "process_files",
        python_callable= func_files_processing
    )

    end = EmptyOperator(
        task_id="end_nse_5_min_dag",
        dag=dag
    )

start >> file_process >> end