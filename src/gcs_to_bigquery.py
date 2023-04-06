"""Flow and Tasks to load data from GCS and store in different BigQuery Tables"""

from google.cloud import bigquery, storage
from prefect import flow, get_run_logger, task
from prefect_gcp.credentials import GcpCredentials

from etl import persist
from utils.config import GCSToBigQueryConfig


@task
def get_unprocessed_search_ids(
    load_dir: str, dataset_id: str, table_id: str
) -> list:
    """ Find unprocessed Search IDs (= files in GCS) which have not been written to Bigquery yet"""
    logger = get_run_logger()

    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")
    project_id = gcp_credentials.project

    # Init GCS Client
    client = storage.Client(project=project_id)

    # Get Search IDs of processed files (= split files in GCS)
    processed_blobs = client.list_blobs("serpapi_jobs", prefix=load_dir)
    processed_search_ids = [
        b.name.split("_")[-1].split(".")[0]
        for b in processed_blobs
        if b.name != f"{load_dir}"
    ]

    # ---------------------

    # Init Bigquery Client
    client = bigquery.Client(project=gcp_credentials.project)

    # Construct Table reference
    table_ref = f"{dataset_id}.{table_id}"

    # Run Query
    if table_id == "search_metadata":
        query = f"SELECT distinct(id) FROM `{project_id}.{table_ref}`"  # Search ID is called "id" in the search_metadata table
    else:
        query = f"SELECT distinct(search_id) FROM `{project_id}.{table_ref}`"

    # Store results in List
    result = client.query(query).result()
    written_search_ids = [row[0] for row in result]

    # ---------------------

    # Find all Search IDs that have not been written to Bigquery yet
    unprocessed_search_ids = list(
        set(processed_search_ids).difference(set(written_search_ids))
    )

    logger.info("INFO level log message")
    logger.info(
        f"There are {len(unprocessed_search_ids)} files which have not been written to Bigquery yet"
    )

    return unprocessed_search_ids


@task
def write_search_metadata_to_bigquery_table(
    load_dir: str,
    file_name: str,
    dataset_id: str,
    table_id: str,
    schema: list,
    autodetect: bool,
):
    """ Write Search Metadata to Bigquery Table """

    logger = get_run_logger()

    persist.write_gcs_json_to_bigquery_table(
        load_dir, file_name, dataset_id, table_id, schema, autodetect
    )

    logger.info("INFO level log message")
    logger.info(
        f"Wrote data from file {file_name} to Bigquery table {dataset_id}.{table_id}"
    )


@task
def write_search_parameters_to_bigquery_table(
    load_dir: str,
    file_name: str,
    dataset_id: str,
    table_id: str,
    schema: list,
    autodetect: bool,
):
    """ Write Search Parameters to Bigquery Table """

    logger = get_run_logger()

    persist.write_gcs_json_to_bigquery_table(
        load_dir, file_name, dataset_id, table_id, schema, autodetect
    )

    logger.info("INFO level log message")
    logger.info(
        f"Wrote data from file {file_name} to Bigquery table {dataset_id}.{table_id}"
    )


@task
def write_job_results_to_bigquery_table(
    load_dir: str,
    file_name: str,
    dataset_id: str,
    table_id: str,
    schema: list,
    autodetect: bool,
):
    """ Write Job Results to Bigquery Table"""

    logger = get_run_logger()

    persist.write_gcs_json_to_bigquery_table(
        load_dir, file_name, dataset_id, table_id, schema, autodetect
    )

    logger.info("INFO level log message")
    logger.info(
        f"Wrote data from file {file_name} to Bigquery table {dataset_id}.{table_id}"
    )


@flow
def gcs_to_bigquery_flow(config: GCSToBigQueryConfig):
    """ Load files from GCS to Bigquery """

    unprocessed_search_metadata_search_ids = get_unprocessed_search_ids(
        load_dir=config.load_dir,
        dataset_id=config.dataset_id,
        table_id="search_metadata",
    )

    schema = [
        bigquery.SchemaField("total_time_taken", "FLOAT", "NULLABLE"),
        bigquery.SchemaField("google_jobs_url", "STRING", "NULLABLE"),
        bigquery.SchemaField("raw_html_file", "STRING", "NULLABLE"),
        bigquery.SchemaField("status", "STRING", "NULLABLE"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", "NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", "NULLABLE"),
        bigquery.SchemaField("json_endpoint", "STRING", "NULLABLE"),
        bigquery.SchemaField("id", "STRING", "NULLABLE"),
    ]

    for i in unprocessed_search_metadata_search_ids:
        file_name = f"search_metadata_{i}"
        write_search_metadata_to_bigquery_table(
            load_dir=config.load_dir,
            file_name=file_name,
            dataset_id=config.dataset_id,
            table_id="search_metadata",
            schema=schema,
            autodetect=False,
        )

    # ----------------------------------------

    unprocessed_search_parameters_search_ids = get_unprocessed_search_ids(
        load_dir=config.load_dir,
        dataset_id=config.dataset_id,
        table_id="search_parameters",
    )

    schema = [
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

    for i in unprocessed_search_parameters_search_ids:
        file_name = f"search_parameters_{i}"
        write_search_parameters_to_bigquery_table(
            load_dir=config.load_dir,
            file_name=file_name,
            dataset_id=config.dataset_id,
            table_id="search_parameters",
            schema=schema,
            autodetect=False,
        )

    # ----------------------------------------

    unprocessed_job_results_search_ids = get_unprocessed_search_ids(
        load_dir=config.load_dir,
        dataset_id=config.dataset_id,
        table_id="job_results",
    )

    # schema=[
    #    bigquery.SchemaField("detected_extension", "JSON", "NULLABLE"),
    #    bigquery.SchemaField("search_id", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("job_id", "BYTES", "NULLABLE"),
    #    bigquery.SchemaField("thumbnail", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("extensions", "STRING", "REPEATED"),
    #    bigquery.SchemaField("via", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("job_highlights", "RECORD", "REPEATED"),
    #    bigquery.SchemaField("related_links", "RECORD", "REPEATED"),
    #    bigquery.SchemaField("title", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("location", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("description", "STRING", "NULLABLE"),
    #    bigquery.SchemaField("company_name", "STRING", "NULLABLE"),
    # ]

    for i in unprocessed_job_results_search_ids:
        file_name = f"job_results_{i}"
        write_job_results_to_bigquery_table(
            load_dir=config.load_dir,
            file_name=file_name,
            dataset_id=config.dataset_id,
            table_id="job_results",
            schema=None,
            autodetect=True,
        )


if __name__ == "__main__":
    gcs_to_bigquery_flow(GCSToBigQueryConfig())
