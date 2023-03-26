"""Python module related to process data"""

import json

def extract_search_metadata(response_dict: dict) -> dict:
   """Extract the metadata from the response_dict

   Parameters
   ----------
   response_dict : dict
      The response_dict to extract the metadata from
   """

   search_metadata = response_dict["search_metadata"]

   return search_metadata

def extract_search_parameters(response_dict: dict) -> dict:
   """Extract the parameters from the response_dict

   Parameters
   ----------
   response_dict : dict
      The response_dict to extract the parameters from
   """

   search_parameters = response_dict["search_parameters"]

   return search_parameters

def extract_job_results(response_dict: dict) -> dict:
   """Extract the job results from the response_dict

   Parameters
   ----------
   response_dict : dict
      The response_dict to extract the job results from
   """

   job_results = response_dict["jobs_results"]

   return job_results

def prepare_data_for_bigquery(response_dict: dict) -> dict:
   """Process the data for BigQuery data ingestion
   
   Parameters
   ----------
   response_dict : dict
      The response_dict to process
   
   """

   search_metadata = extract_search_metadata(response_dict)
   search_parameters = extract_search_parameters(response_dict)
   job_results = extract_job_results(response_dict)

   prepared_data = {
      u"search_metadata": {
        "id": "64201eb234ff954f9eb2f476",
        "status": "Success"
    },
      u"search_parameters": {
        "q": "Data Analyst",
        "engine": "google_jobs"
    },
      u"job_results": {
         "title": "Data Analyst (m/w/d)",
         "company_name": "F mal s GmbH",
        }
   }

   return prepared_data