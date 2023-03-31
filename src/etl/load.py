"""Python module related to load data"""

from prefect_gcp.credentials import GcpCredentials
from google.cloud import storage

def load_file_into_memory(
        load_dir: str, file_name: str, extension: str, load_location: str = "local"
):

    """Load a file to stream

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
    """

    # Construct the load file path
    file_name_with_suffix_and_extension = f"{file_name}.{extension}"
    load_path = f"{load_dir}/{file_name_with_suffix_and_extension}"

    if load_location == "gcs":

      # Load Credentials and Config
      gcp_credentials = GcpCredentials.load("gcp-credentials")
      project_id = gcp_credentials.project

      # Init Client
      client = storage.Client(project=project_id)
      gcs_bucket = client.get_bucket("serpapi_jobs")    

      # Download from GCS
      blob = gcs_bucket.blob(load_path)
      bytes = blob.download_as_bytes()

      return bytes

