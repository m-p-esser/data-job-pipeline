"""Python module related to requesting data from APIs"""

import requests


def request_serpapi(params: dict) -> requests.Response:
    """
    Request the API

    Parameters
    ----------
    params : dict
        The parameters to pass to the API
    """

    response = requests.get("https://serpapi.com/search", params=params)

    # response = GoogleSearch(params)
    
    try:

        response.raise_for_status()

    except requests.exceptions.HTTPError as error:
        print(f"HTTP error occurred: {error}")
    except requests.exceptions.ConnectionError as error:
        print(f"Connection error occurred: {error}")
    except requests.exceptions.Timeout as error:
        print(f"Timeout error occurred: {error}")
    except requests.exceptions.RequestException as error:
        print(f"An error occurred: {error}")

    return response


def parse_response(response: requests.Response) -> dict:
    """Extract data from response and return as dict

    Parameters
    ----------
    response : requests.Response
    """

    response_dict = response.json()

    return response_dict

