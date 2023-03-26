""" Module to create a BigQuery table from Schema"""

from google.cloud import bigquery
from prefect_gcp.credentials import GcpCredentials

from utils.data_models import BigQueryField, BigQuerySchema


def create_bigquery_dataset(dataset_id: str):
    """
    Creates a BigQuery Dataset.

    Args:
        dataset_id: The name of the dataset to be created.
    """
    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")
    project_id = gcp_credentials.project

    # Init Client
    client = bigquery.Client(project=gcp_credentials.project)

    # Construct a full Dataset object to send to the API
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)

    # Make an API request to create dataset
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {client.project}.{dataset.dataset_id}")


def create_bigquery_table(
    dataset_id: str, table_name: str, fields: list[bigquery.SchemaField]
):
    """
    Creates a BigQuery Table.

    Args:
        dataset_id: BiqQuerySchema Data Model.
        table_name: The name of the table to be created.
        fields: The fields of the table to be created.
    """
    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")
    project_id = gcp_credentials.project

    # Init Client
    client = bigquery.Client(project=gcp_credentials.project)

    # Construct a full Table object to send to the API
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, fields)

    # Make an API request to create table
    client.create_dataset(dataset_id, exists_ok=True)
    table = client.create_table(table)
    print(f"Created table {project_id}.{dataset_id}.{table_name}")


if __name__ == "__main__":

    # Instantiate Domain Summary Schema
    domain_summary_bigquery_schema = BigQuerySchema(
        dataset_id="raw",
        table_name="google_jobs",
        fields=[
            BigQueryField(name="id", type="STRING", mode="REQUIRED", default_value_expression="GENERATE_UUID()"),
            BigQueryField(name="search_metadata", type="JSON", mode="REQUIRED"),
            # BigQueryField(name="search_parameters", type="JSON", mode="REQUIRED"),
            # BigQueryField(name="job_results", type="JSON", mode="REQUIRED")
        ],
    )
    fields = domain_summary_bigquery_schema.format_schema()

    # Create Bigquery Dataset
    create_bigquery_dataset(
        dataset_id=domain_summary_bigquery_schema.dataset_id
    )

    # Create Domain Summary Bigquery Table
    create_bigquery_table(
        dataset_id=domain_summary_bigquery_schema.dataset_id,
        table_name=domain_summary_bigquery_schema.table_name,
        fields=fields,
    )
