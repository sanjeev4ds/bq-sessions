from datetime import datetime, timezone, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from google.cloud import bigquery

GCP_CONN_ID= "google_cloud_connection_2"

from requirement.main_requirements import get_project,get_bigquery_raw_table_temp,get_gcs_temp_landing,get_bigquery_consume_table,  get_input_bucket, get_gcs_uri_landing, get_bigquery_raw_table,get_input_directory, get_output_landing_directory, get_moving_directory
PROJECT_ID = get_project()
OUTPUT_RAW_BIGQUERY_TABLE = get_bigquery_raw_table()
RAW_TEMP_TABLE= get_bigquery_raw_table_temp()
TEMP_GCS_LOCATION= get_gcs_temp_landing()
BIGQUERY_CONSUME_TABLE = get_bigquery_consume_table()
# INPUT_LANDING_GCS_URI = get_gcs_uri_landing()

#yesterday date value
# yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

def func_files_processing():

    DATASET_ID = BIGQUERY_CONSUME_TABLE.split(".")[1]
    TABLE_ID = BIGQUERY_CONSUME_TABLE.split(".")[2]

    schema = [
        bigquery.SchemaField("stock_symbol", "STRING"),
        bigquery.SchemaField("datetime", "DATETIME"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("raw_cre_tms", "TIMESTAMP"),
        bigquery.SchemaField("raw_chng_tms", "TIMESTAMP"),
        bigquery.SchemaField("rec_cre_tms", "TIMESTAMP"),
        bigquery.SchemaField("rec_chng_tms", "TIMESTAMP")
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
            type_=bigquery.TimePartitioningType.MONTH,
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
            source_format = bigquery.SourceFormat.PARQUET,
            # skip_leading_rows = 1,
            autodetect = False,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # source_file = os.path.join(directory_path, file)
        # source_file_bytes = open(source_file, "rb")
        load_job = client.load_table_from_uri(
            TEMP_GCS_LOCATION,
            table_ref,
            job_config= job_config,
        )
        load_job.result()
        print("successfully inserted")
    except Exception as e:
        print(e)
        print("failed to insert")

default_args = {
    # "start_date": yesterday,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes= 5)
}

with DAG(
    dag_id = "DAG3",
    catchup= False,
    schedule_interval = None, #timedelta(days =1),
    default_args= default_args
) as dag:

    start = EmptyOperator(
        task_id="start_nse_5_min_dag3",
        dag = dag
    )

    create_temp_table = BigQueryInsertJobOperator(
        task_id="run_query_to_temp_table",
        configuration={
            "query": {
                "query": f"""
                            create or replace table `{RAW_TEMP_TABLE}`
                                partition by date_trunc(datetime, MONTH)
                                cluster by stock_symbol
                                As
                                (
                                  Select 
                                    stock_symbol,datetime, open, high, low, close, volume, 
                                    rec_cre_tms as raw_cre_tms,
                                    rec_chng_tms raw_chng_tms, 
                                    current_timestamp() as rec_cre_tms,
                                    current_timestamp() as rec_chng_tms
                                  From `{OUTPUT_RAW_BIGQUERY_TABLE}`
                                );
                        """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Step 2: Export that temp table to GCS in Parquet format
    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_selected_columns_to_gcs",
        source_project_dataset_table=f"{RAW_TEMP_TABLE}",
        destination_cloud_storage_uris=[TEMP_GCS_LOCATION],
        export_format="PARQUET",
        gcp_conn_id=GCP_CONN_ID,
    )

    file_process = PythonOperator(
        task_id="process_files",
        python_callable=func_files_processing
    )

    end = EmptyOperator(
        task_id="end_nse_5_min_dag3",
        dag=dag
    )

start >> create_temp_table >> export_to_gcs >> file_process >> end