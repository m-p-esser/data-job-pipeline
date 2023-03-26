"""Flow and Tasks to request Google Jobs API Endpoint from Serpapi"""

import requests
from prefect import flow, get_run_logger, task

from etl import persist, request, process
from utils.config import GoogleJobsAPIRequestConfig
from utils.data_models import BigQuerySchema


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
    response_dict: dict, save_dir: str, file_name: str, save_location: str
) -> None:
    """Save the result from the Google Jobs API Endpoint (Serpapi)"""

    logger = get_run_logger()

    persist.save_result_as_file(response_dict, save_dir, file_name, save_location)

    logger.info("INFO level log message")
    logger.info(f"Saved API response here: {save_dir}/{file_name} | ({save_location})")


# @task
# def write_google_jobs_endpoint_result_to_bigquery(
#     response_dict: dict, dataset_id: str, table_name: str
# ) -> None:
#     """Write the result from the Google Jobs API Endpoint (Serpapi) to BigQuery"""

#     logger = get_run_logger()
#     logger.info("INFO level log message")

#     # Prepare rows for BigQuery
#     # prepared_data = process.prepare_data_for_bigquery(response_dict)
#     # rows = []
#     # rows.append(prepared_data)

#     rows = [{
#         "search_metadata": {
#             "id": "64201eb234ff954f9eb2f476",
#             "status": "Success"
#         },
#         "search_parameters": {
#             "q": "Data Analyst",
#             "engine": "google_jobs"
#         },
#         "job_results": {
#             "title": "Data Analyst (m/w/d)",
#             "company_name": "F mal s GmbH",
#         }
#         }]

#     logger.info(f"The following rows will be written to table: {rows}")

#     # Write to BigQuery
#     persist.write_to_bigquery_table(dataset_id, table_name, rows)

#     logger.info(f"Inserted rows to table: {dataset_id}.{table_name}")

@flow
def google_jobs_endpoint_request_flow(
    config: GoogleJobsAPIRequestConfig
):
    """Flow to request the Domain Summary Serpstat API endpoint and store the results"""

    response = request_google_jobs_endpoint(config.params)

    result = parse_google_jobs_endpoint_response(response)

    save_google_jobs_endpoint_result(
        result, config.save_dir, config.file_name, config.save_location
    )

    # write_google_jobs_endpoint_result_to_bigquery(result, config.dataset_id, config.table_name)


if __name__ == "__main__":
    google_jobs_endpoint_request_flow(
        config=GoogleJobsAPIRequestConfig(),
    )
