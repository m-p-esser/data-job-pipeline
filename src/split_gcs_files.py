"""Flow and Tasks to split raw GCS files into individual files"""

import json

from prefect import flow, get_run_logger, task

from etl import load, persist
from utils.config import GCSFileSplittingConfig


@task
def split_gcs_file_into_individual_files(load_dir: str, file_name: str, extension: str = 'json', load_location: str = 'gcs') -> dict[str, list]:
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
    bytes = load.load_file_into_memory(load_dir, file_name, extension, load_location)

    # Transform Bytes to dict 
    data = json.loads(bytes.decode('utf-8'))

    # Split data into different dicts
    metadata = data["search_metadata"]
    search_parameters = data["search_parameters"]
    job_results = data["jobs_results"]

    # Combine different Dicts in a single Dict
    splitted_data = {'metadata': metadata, 'search_parameters': search_parameters, 'job_results': job_results}

    logger.info("INFO level log message")
    logger.info(f"Split GCS file into {len(splitted_data.keys())} individual files")

    return splitted_data

@task
def save_splitted_files_in_gcs(
    splitted_data: dict[str, list], save_dir: str, extension: str = 'json', save_location: str = 'gcs'
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

    id = splitted_data["metadata"]["id"] # search id

    for k, v in splitted_data.items():

        # Store individual dicts as file in GCS
        file_name_with_suffix = f"{k}_{id}"
        persist.save_result_as_file(v, save_dir, file_name_with_suffix, extension, save_location)

        logger.info("INFO level log message")
        logger.info(f"Saved File here: {save_dir}/{file_name_with_suffix} | ({save_location})")


@flow
def split_gcs_files_flow(
    config: GCSFileSplittingConfig
):
    """Flow to request the Domain Summary Serpstat API endpoint and store the results"""

    splitted_data = split_gcs_file_into_individual_files(config.load_dir, "google_jobs_64272d609c84ac2f3119a48e")

    save_splitted_files_in_gcs(splitted_data, config.save_dir)

if __name__ == "__main__":
    split_gcs_files_flow(
        config=GCSFileSplittingConfig(),
    )