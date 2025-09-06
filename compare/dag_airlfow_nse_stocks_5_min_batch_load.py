import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

#yesterday date value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    "start_date": yesterday,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes= 5)
}

with DAG(
    dag_id = "Dag_Airflow_1_GCS_to_BQ",
    catchup= False,
    schedule_interval = timedelta(days =1),
    default_args= default_args
) as dag:

    start = EmptyOperator(
        task_id="start_first_dag",
        dag = dag
    )

    gcs_to_bq_load = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_load",
        bucket = "sanjeev-project-raw",
        source_objects = ["auto_data/USA_cars_datasets.csv"],
        destination_project_dataset_table= "bq-sessions-sanjeev.sanjeev_project_raw.airflow_table",
        schema_fields = [
            {'name': 'index', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},

            {'name': 'title_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'mileage', 'type': 'DECIMAL', 'mode': 'NULLABLE'},
            {'name': 'color', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'vin', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lot', 'type': 'INTEGER', 'mode': 'NULLABLE'},

            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'condition', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        source_format= 'CSV',
        field_delimiter= ',',
        skip_leading_rows= 1,
        create_disposition= 'CREATE_IF_NEEDED',
        write_disposition= 'WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_connection_2',
        # bigquery_conn_id= 'google_cloud_default',
        # google_cloud_storage_conn_id= 'google_cloud_default',
        dag= dag
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id = "trigger_Dag_Airflow_2_BQ_to_BQ",
        trigger_dag_id = "Dag_Airflow_2_BQ_to_BQ",
        conf={"message":"Trigger Dag Second"}
    )

    end = EmptyOperator(
        task_id="end_first_dag",
        dag=dag
    )

start  >>  gcs_to_bq_load >> trigger_second_dag >> end