"""Flow and Tasks to split raw GCS files into individual files"""

import json

import pandas as pd
from google.cloud import storage
from prefect import flow, get_run_logger, task
from prefect_gcp.credentials import GcpCredentials

from etl import load, persist
from utils.config import GCSFileSplittingConfig


@task
def get_unprocessed_search_ids(load_dir: str, save_dir: str) -> list:

    logger = get_run_logger()

    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")
    project_id = gcp_credentials.project

    # Init Client
    client = storage.Client(project=project_id)

    # Get Search IDs of raw files (which might or might not have been split yet)
    raw_blobs = client.list_blobs("serpapi_jobs", prefix=load_dir)
    raw_search_ids = [
        b.name.split("_")[-1].split(".")[0]
        for b in raw_blobs
        if b.name != f"{load_dir}/"
    ]

    # Get Search IDs of processed files (which have already been split)
    processed_blobs = client.list_blobs("serpapi_jobs", prefix=save_dir)
    processed_search_ids = [
        b.name.split("_")[-1].split(".")[0]
        for b in processed_blobs
        if b.name != f"{save_dir}"
    ]

    # Find all Search IDs that have not been processed yet (= files that have not been split yet)
    unprocessed_search_ids = list(
        set(raw_search_ids).difference(set(processed_search_ids))
    )

    logger.info("INFO level log message")
    logger.info(
        f"There are {len(unprocessed_search_ids)} raw files which have not been split into multiple files yet"
    )

    return unprocessed_search_ids


@task
def split_gcs_file_into_individual_files(
    load_dir: str,
    file_name: str,
    extension: str = "json",
    load_location: str = "gcs",
) -> dict[str, list]:
    """Split a GCS file into individual files

    Parameters
    ----------
    load_dir : str
        The directory to load the file from
    file_name : str
        The name of the file to load
    extension : str, optional
        The extension of the file, by default 'json'
    load_location : str, optional
        The location to load the file from, by default 'gcs'

    """

    logger = get_run_logger()

    # Load GCS file into memory
    bytes = load.load_file_into_memory(
        load_dir, file_name, extension, load_location
    )

    # Transform Bytes to dict
    data = json.loads(bytes.decode("utf-8"))

    # Split data into different dicts
    search_metadata = data["search_metadata"]
    search_parameters = data["search_parameters"]
    job_results = data["jobs_results"]

    # Combine different Dicts in a single Dict
    splitted_data = {
        "search_metadata": search_metadata,
        "search_parameters": search_parameters,
        "job_results": job_results,
    }

    logger.info("INFO level log message")
    logger.info(
        f"Split GCS file into {len(splitted_data.keys())} individual files"
    )

    return splitted_data


@task
def save_splitted_files_in_gcs(
    splitted_data: dict[str, list],
    save_dir: str,
    extension: str = "json",
    save_location: str = "gcs",
) -> None:

    """Save splitted GCS files in GCS

    Parameters
    ----------
    splitted_data : dict[str, list]
        The splitted data to save
    save_dir : str
        The directory to save the files in
    extension : str, optional
        The extension of the files, by default 'json'
    save_location : str, optional
        The location to save the files in, by default 'gcs'

    """

    logger = get_run_logger()

    search_id = splitted_data["search_metadata"]["id"]

    for k, v in splitted_data.items():

        try:
            # Store individual dicts as file in GCS
            file_name_with_suffix = f"{k}_{search_id}"

            if k == "search_parameters":
                v.update({"search_id": search_id})

            if k == "job_results":
                for i in v:
                    i.update({"search_id": search_id})
                    i.pop("detected_extensions", None)

                # Convert to JSON New line delimited format
                data = pd.DataFrame(v).to_json(orient="records", lines=True)
                persist.save_result_as_file(
                    data,
                    save_dir,
                    file_name_with_suffix,
                    extension,
                    save_location,
                    indent=None,
                )

            if k != "job_results":
                persist.save_result_as_file(
                    v,
                    save_dir,
                    file_name_with_suffix,
                    extension,
                    save_location,
                    indent=None,
                )

        except Exception as e:
            logger.error("ERROR level log message")
            logger.error(f"Error while saving file: {e}")

        logger.info("INFO level log message")
        logger.info(
            f"Saved File here: {save_dir}/{file_name_with_suffix} | ({save_location})"
        )


@flow
def split_gcs_files_flow(config: GCSFileSplittingConfig):
    """Flow to request the Domain Summary Serpstat API endpoint and store the results"""

    logger = get_run_logger()

    unprocessed_search_ids = get_unprocessed_search_ids(
        config.load_dir, config.save_dir
    )

    for search_id in unprocessed_search_ids:
        try:
            file_name = f"google_jobs_{search_id}"

            splitted_data = split_gcs_file_into_individual_files(
                config.load_dir, file_name
            )

            save_splitted_files_in_gcs(splitted_data, config.save_dir)

        except Exception as e:
            logger.error("ERROR level log message")
            logger.error(f"Error while splitting file: {e}")


if __name__ == "__main__":
    split_gcs_files_flow(
        config=GCSFileSplittingConfig(),
    )
