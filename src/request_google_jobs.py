"""Flow and Tasks to request Google Jobs API Endpoint from Serpapi"""

import requests
from prefect import flow, get_run_logger, task

from etl import persist, request
from utils.config import GoogleJobsAPIRequestConfig, GoogleJobsAPIQueryCombinations, create_permutations


@task(
    retries=0,
    retry_delay_seconds=10,
    name="Request Google Jobs API (Serpapi)",
    description="Request Google Jobs API Endpoint from Serpapi (https://serpapi.com/google-jobs-api)",
)
def request_google_jobs_endpoint(
    params: dict
) -> requests.Response:
    """Request Google Jobs API Endpoint from Serpapi (https://serpapi.com/google-jobs-api)"""

    logger = get_run_logger()

    response = request.request_serpapi(params)

    logger.info("INFO level log messages")
    logger.info(f"Requested API endpoint:")
    logger.info(f"Using the following parameters: {params}")

    logger.info(f"Response: {response}")

    return response


@task
def parse_google_jobs_endpoint_response(
    response: requests.Response,
) -> dict:
    """Parse the response from the Google Jobs API Endpoint (Serpapi)"""

    logger = get_run_logger()

    response_dict = request.parse_response(response)

    logger.info("INFO level log messages")
    logger.info("Parsed the following API Response:")
    # logger.info(response_dict)

    return response_dict


@task
def save_google_jobs_endpoint_result(
    response_dict: dict, save_dir: str, file_name: str, extension: str, save_location: str
) -> None:
    """Save the result from the Google Jobs API Endpoint (Serpapi)"""

    logger = get_run_logger()

    # Define the Path where the file will be saved
    if response_dict["search_metadata"]["status"] == "Success":
        save_dir = f"{save_dir}/successful"

    if response_dict["search_metadata"]["status"] == "Error":
        save_dir = f"{save_dir}/error"

    file_name_with_suffix = file_name + "_" + response_dict["search_metadata"]["id"]

    persist.save_result_as_file(response_dict, save_dir, file_name_with_suffix, extension, save_location)

    logger.info("INFO level log message")
    logger.info(f"Saved API response here: {save_dir}/{file_name_with_suffix} | ({save_location})")


@flow
def google_jobs_endpoint_request_flow(
    request_config: GoogleJobsAPIRequestConfig,
    combinations_config: GoogleJobsAPIQueryCombinations,
):
    """Flow to request the Domain Summary Serpstat API endpoint and store the results"""

    # Create all possible combinations of the parameters
    permutations = create_permutations(combinations_config.jobs, combinations_config.locations, combinations_config.start_offsets)

    # Request the API Endpoint for each combination of parameters
    for i in permutations:
        (q, location, start) = i
        request_config.params.update({"q": q})
        request_config.params.update({"location": location})
        request_config.params.update({"start": start})

        response = request_google_jobs_endpoint(request_config.params)

        result = parse_google_jobs_endpoint_response(response)

        save_google_jobs_endpoint_result(
            result, request_config.save_dir, request_config.file_name, request_config.extension, request_config.save_location
        )

if __name__ == "__main__":
    google_jobs_endpoint_request_flow(
        request_config=GoogleJobsAPIRequestConfig(),
        combinations_config=GoogleJobsAPIQueryCombinations()
    )
