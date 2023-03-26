"""Python module related to persist data"""

import json

from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.credentials import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery


def save_result_as_file(
    response_dict: dict, save_dir: str, file_name: str, save_location: str = "local"
):
    """Save the response_dict to a file

    Parameters
    ----------
    response_dict : dict
        The response_json to save
    save_dir : str
        The directory to save the response_dict
    save_location : str, optional
        The location to store the response_dict, by default "local"
    """

    allowed_save_locations = ["local", "gcs"]
    if save_location not in allowed_save_locations:
        raise ValueError(
            "save_location must be one of the following values: {}".format(
                allowed_save_locations
            )
        )

    # Construct the file name
    file_name_with_suffix = file_name + "_" + response_dict["search_metadata"]["id"]
    file_name_with_suffix_and_extension = file_name_with_suffix + ".json"

    # Define the Path where the file will be saved
    if response_dict["search_metadata"]["status"] == "Success":
        save_path = f"{save_dir}/successful/{file_name_with_suffix_and_extension}"

    if response_dict["search_metadata"]["status"] == "Error":
        save_path = f"{save_dir}/error/{file_name_with_suffix_and_extension}"

    # Write to location depending on save_location
    if save_location == "local":
        with open(save_path, "w") as f:
            json.dump(response_dict, f, indent=4)

    if save_location == "gcs":

        # Load Credentials and Config
        gcp_credentials = GcpCredentials.load("gcp-credentials")
        project_id = gcp_credentials.project

        # Init Client
        client = storage.Client(project=project_id)
        gcs_bucket = client.get_bucket("serpapi_jobs")

        # Upload to GCS
        blob = gcs_bucket.blob(save_path)
        blob.upload_from_string(
            data=json.dumps(response_dict), 
            content_type="application/json"
            )

def write_to_bigquery_table(
    dataset_id: str, table_name: str, row_to_insert: dict
):
    """
    Creates a BigQuery Table.

    Args:
        dataset_id: Dataset ID of the table where data should be written to.
        table_name: The name of the table where data should be written to.
        rows_to_insert: The rows to insert into the table.
    """
    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")

    # Init Client
    client = bigquery.Client(project=gcp_credentials.project)

    # Get Table instance
    table_ref = f"{dataset_id}.{table_name}"
    table = client.get_table(table_ref)

    # Insert Row
    client.insert_rows(table, row_to_insert)



# def transfer_data_from_gcs_to_bigquery(
#     dataset_id: str, table_name: str, gcs_uri: str
# ):
#     """Load data from file stored in Google Cloud Storage (GCS) and store it in Google BigQuery Table

#     Parameters
#     ----------
#     dataset_id : str
#         The name of the dataset
#     table_name : str
#         The name of the table
#     gcs_uri : str
#         The URI of the file in GCS
#     """

#     logger = get_run_logger()

#     # Load GCP credentials from the context
#     gcp_credentials = GcpCredentials.load("gcp-credentials")

#     # Load data from GCS to BigQuery
#     result = bigquery_load_cloud_storage(
#         dataset=dataset_id,
#         table=table_name,
#         uri=gcs_uri,
#         gcp_credentials=gcp_credentials,
#     )

#     logger.info("INFO level log message")
#     logger.info(
#         f"Inserted API response to BigQuery here: {dataset_id}.{table_name}"
#     )

#     return result
