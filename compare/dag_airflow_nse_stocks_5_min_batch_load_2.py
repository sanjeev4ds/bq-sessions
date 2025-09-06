from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

@dag(
    dag_id = "Dag_Airflow_3_nse_5_min_batch_load",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["google_cloud", "bigquery", "dynamic_tasks"],
)
def dynamic_gcs_to_bq():

    @task
    def get_gcs_files_to_load():
        files_to_load = [
{"source_objects": ["NSE_stocks_5_min_sample/MOTHERSON_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.MOTHERSON_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SBILIFE_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SBILIFE_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BAJFINANCE_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BAJFINANCE_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/GODREJCP_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.GODREJCP_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/APOLLOHOSP_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.APOLLOHOSP_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/LT_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.LT_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/WIPRO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.WIPRO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TECHM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TECHM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BAJAJFINSV_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BAJAJFINSV_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/POWERGRID_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.POWERGRID_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TRENT_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TRENT_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HEROMOTOCO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HEROMOTOCO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BEL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BEL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/IRFC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.IRFC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ADANIENSOL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ADANIENSOL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HDFCBANK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HDFCBANK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TCS_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TCS_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ADANIPORTS_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ADANIPORTS_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/COALINDIA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.COALINDIA_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/DLF_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.DLF_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/AXISBANK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.AXISBANK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/NIFTY BANK_5minutedata.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.NIFTY BANK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TATAPOWER_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TATAPOWER_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/NESTLEIND_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.NESTLEIND_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/INDUSINDBK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.INDUSINDBK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ZYDUSLIFE_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ZYDUSLIFE_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BPCL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BPCL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/GAIL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.GAIL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SBIN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SBIN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/PIDILITIND_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.PIDILITIND_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ONGC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ONGC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ASIANPAINT_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ASIANPAINT_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TORNTPHARM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TORNTPHARM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SHRIRAMFIN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SHRIRAMFIN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TITAN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TITAN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/DRREDDY_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.DRREDDY_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ETERNAL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ETERNAL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/CHOLAFIN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.CHOLAFIN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/KOTAKBANK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.KOTAKBANK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/JIOFIN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.JIOFIN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/INDIGO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.INDIGO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/MM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.MM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ICICIGI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ICICIGI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/EICHERMOT_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.EICHERMOT_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HYUNDAI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HYUNDAI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/JINDALSTEL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.JINDALSTEL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/NTPC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.NTPC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/JSWENERGY_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.JSWENERGY_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HINDUNILVR_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HINDUNILVR_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BAJAJHLDNG_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BAJAJHLDNG_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/INFY_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.INFY_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ITC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ITC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/PNB_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.PNB_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ULTRACEMCO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ULTRACEMCO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/UNITDSPR_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.UNITDSPR_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BAJAJHFL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BAJAJHFL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/LICI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.LICI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BAJAJ-AUTO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BAJAJ-AUTO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/JSWSTEEL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.JSWSTEEL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/DMART_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.DMART_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/MARUTI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.MARUTI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ICICIBANK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ICICIBANK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SHREECEM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SHREECEM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HAVELLS_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HAVELLS_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/INDHOTEL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.INDHOTEL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/RECLTD_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.RECLTD_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HAL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HAL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/PFC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.PFC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/IOC_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.IOC_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TVSMOTOR_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TVSMOTOR_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TATAMOTORS_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TATAMOTORS_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SIEMENS_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SIEMENS_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/LODHA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.LODHA_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ADANIGREEN_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ADANIGREEN_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SWIGGY_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SWIGGY_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/DABUR_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.DABUR_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/DIVISLAB_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.DIVISLAB_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/CIPLA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.CIPLA_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/RELIANCE_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.RELIANCE_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ABB_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ABB_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/SUNPHARMA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.SUNPHARMA_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ADANIENT_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ADANIENT_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BHARTIARTL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BHARTIARTL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ICICIPRULI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ICICIPRULI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/CGPOWER_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.CGPOWER_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HCLTECH_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HCLTECH_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/LTIM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.LTIM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/VEDL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.VEDL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HINDALCO_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HINDALCO_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/GRASIM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.GRASIM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/ADANIPOWER_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.ADANIPOWER_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/VBL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.VBL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/HDFCLIFE_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.HDFCLIFE_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/CANBK_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.CANBK_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TATACONSUM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TATACONSUM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/NIFTY 50_5minutedata.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.NIFTY 50_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BOSCHLTD_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BOSCHLTD_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BANKBARODA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BANKBARODA_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/AMBUJACEM_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.AMBUJACEM_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/TATASTEEL_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.TATASTEEL_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/NAUKRI_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.NAUKRI_5MIN"},
{"source_objects": ["NSE_stocks_5_min_sample/BRITANNIA_5minute.csv"], "destination_project_dataset_table": "bq-sessions-sanjeev:sanjeev_project_nse_raw.BRITANNIA_5MIN"},
]
        return files_to_load

    # Get the list of files from the Python task
    files_list = get_gcs_files_to_load()

    load_gcs_to_bq = (GCSToBigQueryOperator.partial(
        task_id="gcs_to_bq_load",
        bucket="sanjeev-project-landing",
        schema_fields=[
            {"name": "date", "type": "DATETIME"},
            {"name": "open", "type": "FLOAT"},
            {"name": "high", "type": "FLOAT"},
            {"name": "low", "type": "FLOAT"},
            {"name": "close", "type": "FLOAT"},
            {"name": "volume", "type": "INTEGER"}
        ],
        source_format="CSV",
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id='google_cloud_connection_2'
    )
    .expand_kwargs(
        files_list
        # source_objects=files_list.map("source_object"),
        # destination_project_dataset_table=files_list.map("destination_project_dataset_table"),
        # items= files_list
    ))

# Instantiate the DAG
dynamic_gcs_to_bq()
