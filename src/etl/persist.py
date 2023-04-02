"""Python module related to persist data"""

import json
import time

from prefect_gcp.credentials import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery
from typing import Union

def save_result_as_file(
    data: Union[dict, str], save_dir: str, file_name: str, extension: str, save_location: str = "local", indent: int = None
):
    """Save the data

    Parameters
    ----------
    data : Union[dict, str]
        The data to save
    save_dir : str
        The directory to save the file to
    file_name : str
        The name of the file to save
    extension : str
        The extension of the file
    save_location : str, optional
        The location to store the file, by default "local"
    indent : int, optional
        The indentation of the JSON file, by default None
    """

    allowed_save_locations = ["local", "gcs"]
    if save_location not in allowed_save_locations:
        raise ValueError(
            f"save_location must be one of the following values: {allowed_save_locations}. \n Instead got: '{save_location}'"
        )

    # Construct the save file path
    file_name_with_suffix_and_extension = f"{file_name}.{extension}"
    save_path = f"{save_dir}/{file_name_with_suffix_and_extension}"

    if save_location == "local":

        with open(save_path, "w") as f:
            json.dump(obj=data, fp=f, indent=indent)

    if save_location == "gcs":

        # Load Credentials and Config
        gcp_credentials = GcpCredentials.load("gcp-credentials")
        project_id = gcp_credentials.project

        # Init Client
        client = storage.Client(project=project_id)
        gcs_bucket = client.get_bucket("serpapi_jobs")
        blob = gcs_bucket.blob(save_path)

        # Prepare data for upload if it is a dict
        if isinstance(data, dict):
            data = json.dumps(obj=data, indent=indent)

        # Upload to GCS
        blob.upload_from_string(
            data=data, 
            content_type="application/json"
            )     

def write_gcs_json_to_bigquery_table(load_dir: str, file_name: str, dataset_id: str, table_id: str): #, schema: list[bigquery.SchemaField]):

    """Load a JSON file stored in GCS and write it to a BigQuery table

    Parameters
    ----------
    load_dir : str
        The directory to load the file from
    file_name : str
        The name of the file to load
    extension : str
        The extension of the file
    load_location : str, optional
        The location to load the file from, by default "local"
    dataset_id : str
        The BigQuery dataset to write to
    table_id : str
        The BigQuery table to write to
    schema : list[bigquery.SchemaField]
        The schema of the BigQuery table
    """

    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")

    # Init Client
    client = bigquery.Client(project=gcp_credentials.project)

    # Construct Table reference
    table_id = f"{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        # schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND"
    )

    # Construct URI
    uri = f"gs://serpapi_jobs/{load_dir}/{file_name}.json"
    job = client.load_table_from_uri(uri, table_id, job_config=job_config)

    # Wait for the job to complete.
    while job.state != "DONE":
        job.reload()
        time.sleep(2)
    print(job.result())