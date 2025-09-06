import os
from datetime import datetime, timezone, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def func_files_processing():
    directory_path = "/Users/sanjeevsaini/PycharmProjects/pythonProject/bq-sessions-usecase-2/dir_input/NSE_stocks_5_min"
    all_files = os.listdir(directory_path)
    for file in all_files:
        print("file:", file)
        # if file == "TATAMOTORS_5minute.csv":
        file_path = os.path.join(directory_path, file)
        print("file_path", file_path)
        with open(file_path, "r") as f:

            content = f.read()
            content_list_complete = content.split("\n")[1:]

            # processing all records in small batches
            #  BigQueryClient.insert_rows_json()
            # Sends data in small batches using the BigQuery streaming insert API, which has:
            # Per-request size limits (~10MB per request).
            # Potential retries causing timeouts for large data.

            len_content_list = len(content_list_complete)
            batch_size = 25000
            for i in range(0, len_content_list, batch_size):
                print("loop start", i)
                list_file = []
                if i+batch_size>len_content_list:
                    content_list = content_list_complete[i:]
                else:
                    content_list = content_list_complete[i: i+batch_size]

                for line in content_list:
                    try:
                        list_line = line.split(",")
                        if len(list_line)==1:
                            continue
                        dict_line = dict()
                        dict_line["datetime"] = datetime.strptime(list_line[0], "%Y-%m-%d %H:%M:%S").isoformat()
                        dict_line["open"] = float(list_line[1])
                        dict_line["high"] = float(list_line[2])
                        dict_line["low"] = float(list_line[3])
                        dict_line["close"] = float(list_line[4])
                        dict_line["volume"] = int(list_line[5])
                        dict_line["filename"] = file
                        dict_line["rec_cre_tms"] = datetime.now(timezone.utc).isoformat()
                        dict_line["rec_chng_tms"] = datetime.now(timezone.utc).isoformat()
                        dict_line["stock_symbol"] = file.split("_")[0]
                        # date,open,high,low,close,volume
                        list_file.append(dict_line)
                    except:
                        print(list_line)
                        continue

                bq_hook = BigQueryHook(gcp_conn_id="google_cloud_connection_2")
                client = bq_hook.get_client()
                print("ingest file", file.split("_")[0], "row", i, "started in BigQuery")
                # project_id = bq_hook.project_id
                table_id = "bq-sessions-sanjeev.sanjeev_project_raw.nse_stocks_5_min"
                client.insert_rows_json(table_id, list_file)
                print("ingested file", file.split("_")[0], "completed row from", i, "to", i+batch_size)
                print("loop end", i)
            # break


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
    dag_id = "Dag_Airflow_3_nse_5_min_shares",
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



