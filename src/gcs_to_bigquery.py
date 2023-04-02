"""Flow and Tasks to load data from GCS and store in different BigQuery Tables"""

from prefect import flow, get_run_logger, task
from google.cloud import bigquery

from etl import persist
from utils.config import GCSToBigQueryConfig


@task
def write_search_metadata_to_bigquery_table(load_dir: str, file_name: str, dataset_id: str, table_id: str, schema: list, autodetect: bool):

   logger = get_run_logger()

   persist.write_gcs_json_to_bigquery_table(load_dir, file_name, dataset_id, table_id, schema, autodetect)
   
   logger.info("INFO level log message")
   logger.info(f"Wrote data to Bigquery table {dataset_id}.{table_id}")

@task
def write_search_parameters_to_bigquery_table(load_dir: str, file_name: str, dataset_id: str, table_id: str, schema: list, autodetect: bool):

   logger = get_run_logger()

   persist.write_gcs_json_to_bigquery_table(load_dir, file_name, dataset_id, table_id, schema, autodetect)
   
   logger.info("INFO level log message")
   logger.info(f"Wrote data to Bigquery table {dataset_id}.{table_id}")

@task
def write_job_results_to_bigquery_table(load_dir: str, file_name: str, dataset_id: str, table_id: str, schema: list, autodetect: bool):

   logger = get_run_logger()

   persist.write_gcs_json_to_bigquery_table(load_dir, file_name, dataset_id, table_id, schema, autodetect)
   
   logger.info("INFO level log message")
   logger.info(f"Wrote data to Bigquery table {dataset_id}.{table_id}")


@flow
def gcs_to_bigquery_flow(config: GCSToBigQueryConfig):

   schema=[
      bigquery.SchemaField("total_time_taken", "FLOAT", "NULLABLE"),
      bigquery.SchemaField("google_jobs_url", "STRING", "NULLABLE"),
      bigquery.SchemaField("raw_html_file", "STRING", "NULLABLE"),
      bigquery.SchemaField("status", "STRING", "NULLABLE"),
      bigquery.SchemaField("processed_at", "TIMESTAMP", "NULLABLE"),
      bigquery.SchemaField("created_at", "TIMESTAMP", "NULLABLE"),
      bigquery.SchemaField("json_endpoint", "STRING", "NULLABLE"),
      bigquery.SchemaField("id", "STRING", "NULLABLE"),
   ]

   write_search_metadata_to_bigquery_table(load_dir=config.load_dir, file_name="search_metadata_64272d609c84ac2f3119a48e", dataset_id=config.dataset_id, table_id="search_metadata", schema=schema, autodetect=False)

   schema=[
      bigquery.SchemaField("search_id", "STRING", "NULLABLE"),
      bigquery.SchemaField("gl", "STRING", "NULLABLE"),
      bigquery.SchemaField("google_domain", "STRING", "NULLABLE"),
      bigquery.SchemaField("location_used", "STRING", "NULLABLE"),
      bigquery.SchemaField("start", "INTEGER", "NULLABLE"),
      bigquery.SchemaField("location_requested", "STRING", "NULLABLE"),
      bigquery.SchemaField("hl", "STRING", "NULLABLE"),
      bigquery.SchemaField("engine", "STRING", "NULLABLE"),
      bigquery.SchemaField("q", "STRING", "NULLABLE"),
   ]

   write_search_parameters_to_bigquery_table(load_dir=config.load_dir, file_name="search_parameters_64272d609c84ac2f3119a48e", dataset_id=config.dataset_id, table_id="search_parameters", schema=schema, autodetect=False)

   # schema=[
   #    bigquery.SchemaField("detected_extension", "FLOAT", "NULLABLE"),
   #    bigquery.SchemaField("google_jobs_url", "STRING", "NULLABLE"),
   #    bigquery.SchemaField("raw_html_file", "STRING", "NULLABLE"),
   #    bigquery.SchemaField("status", "STRING", "NULLABLE"),
   #    bigquery.SchemaField("processed_at", "TIMESTAMP", "NULLABLE"),
   #    bigquery.SchemaField("created_at", "TIMESTAMP", "NULLABLE"),
   #    bigquery.SchemaField("json_endpoint", "STRING", "NULLABLE"),
   #    bigquery.SchemaField("id", "STRING", "NULLABLE"),
   # ]

   write_job_results_to_bigquery_table(load_dir=config.load_dir, file_name="job_results_64272d609c84ac2f3119a48e", dataset_id=config.dataset_id, table_id="job_results", schema=None, autodetect=True)


if __name__ == "__main__":
   gcs_to_bigquery_flow(
      GCSToBigQueryConfig()
   )