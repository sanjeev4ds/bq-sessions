
import os
from datetime import datetime, timezone, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import subprocess

def run_bash_command(command):
    # command : mv /Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-2/dir_input/NSE_stocks_5_min_again/file.csv /Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-2/dir_output/processed/NSE_stocks_5_min_sample/"
    try:
        print("shell script before execution")
        result = subprocess.run(command, shell= True, capture_output=True, text=True)
        print(result.stderr)
        print(result.stdout)
        print("shell script after execution")
        return True
    except:
        return False

def transfer_file(file_path):
    if os.path.exists(file_path):
        file_name = file_path.split("/")[-1]
        destination_directory = "/Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-2/dir_output/processed/NSE_stocks_5_min_again/"
        command = f"/bin/mv {file_path} {destination_directory};"
        command+= f"echo 'Successfully transferred the {file_name} to processed {destination_directory};'"
        result = run_bash_command(command)
        if result:
            print("successfully done")
            return True
        else:
            print("not transferred")
            return False

def func_files_processing():
    directory_path = "/Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-2/dir_input/NSE_stocks_5_min_again"
    all_files = os.listdir(directory_path)

    PROJECT_ID = "bq-sessions-sanjeev"
    DATASET_ID = "sanjeev_project_nse_raw_again_4"

    schema = [
        bigquery.SchemaField("date", "DATETIME"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER")
    ]

    for file in all_files:
        TABLE_ID = file.split("_")[0]
        print("started table id:", TABLE_ID)

        # Creating a file table first
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_connection_2")
        client = bq_hook.get_client(project_id=PROJECT_ID)
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
        table_ref = bigquery.TableReference(dataset_ref, TABLE_ID)

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

        source_file = os.path.join(directory_path, file)
        source_file_bytes = open(source_file, "rb")
        load_job = client.load_table_from_file(
            source_file_bytes,
            table_ref,
            job_config= job_config,
        )
        load_job.result()
        print("successfully inserted", TABLE_ID)

        transfer_file_result = transfer_file(source_file)
        print("successfully transferred file into processed directory status", transfer_file_result)

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
    dag_id = "Dag_Airflow_3_nse_5_min_shares_batchload_with_tablecreate_transferfile",
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