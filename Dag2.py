from datetime import datetime, timezone, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Retrieve required names
from requirement.main_requirements import get_project, get_input_bucket, get_gcs_uri_landing, get_bigquery_raw_table,get_input_directory, get_output_landing_directory, get_moving_directory
PROJECT_ID = get_project()
INPUT_BUCKET= get_input_bucket()
INPUT_LANDING_GCS_URI = get_gcs_uri_landing()
OUTPUT_RAW_BIGQUERY_TABLE = get_bigquery_raw_table()

def func_files_processing():

    DATASET_ID = OUTPUT_RAW_BIGQUERY_TABLE.split(".")[1]
    TABLE_ID = OUTPUT_RAW_BIGQUERY_TABLE.split(".")[2]

    schema = [
        bigquery.SchemaField("datetime", "DATETIME"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("rec_cre_tms", "TIMESTAMP"),
        bigquery.SchemaField("rec_chng_tms", "TIMESTAMP"),
        bigquery.SchemaField("stock_symbol", "STRING")
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
            field="datetime"
        )
        try:
            client.create_table(table)
            print("table created")
        except Exception as e:
            print(e)
            print("table already exist")

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
            INPUT_LANDING_GCS_URI,
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
    dag_id = "DAG2",
    catchup= False,
    schedule_interval = None, #timedelta(days =1),
    default_args= default_args
) as dag:

    start = EmptyOperator(
        task_id="start_nse_5_min_dag2",
        dag = dag
    )

    file_process = PythonOperator(
        task_id = "process_files",
        python_callable= func_files_processing
    )

    end = EmptyOperator(
        task_id="end_nse_5_min_dag2",
        dag=dag
    )

start >> file_process >> end