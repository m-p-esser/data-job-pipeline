""" Flow and Tasks to engineer and extract features from raw Bigquery data and load them in final tables """

import re

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.credentials import GcpCredentials

from etl import engineer, persist, process
from utils.config import KeywordExtractionConfig, RegexConfig


@task
def load_job_results(gcp_credentials: GcpCredentials) -> pd.DataFrame:
    """ Load Job Results Dataframe from Bigquery"""

    logger = get_run_logger()

    query = """
   SELECT * FROM `raw.job_results`
   """
    df = pd.read_gbq(
        query, project_id=gcp_credentials.project, dialect="standard"
    )

    logger.info("INFO level log message")
    logger.info(
        f"Loaded Job Results Dataframe with Shape: '{df.shape}' from Bigquery"
    )

    return df


@task
def extract_keywords_from_job_descriptions(
    df: pd.DataFrame, config: KeywordExtractionConfig
) -> pd.DataFrame:
    """ Extract keywords from job descriptions """

    logger = get_run_logger()

    config_dict = config.__dict__

    df_keywords = pd.DataFrame()
    for k, v in config_dict.items():
        try:
            df_temp = pd.DataFrame(
                {
                    "job_id": df["job_id"].tolist(),
                    k: [
                        engineer.extract_keywords(v, d)
                        for d in df["description"].tolist()
                    ],
                }
            )
            df_keywords = pd.concat([df_keywords, df_temp], axis=1)

        except Exception as e:
            logger.error("ERROR level log message")
            logger.error(f"Error while saving file: {e}")

    df_keywords = df_keywords.loc[:, ~df_keywords.columns.duplicated()].copy()

    logger.info("INFO level log message")
    logger.info(
        f"Created Keyword Dataframe with Shape: '{df_keywords.shape}' from Bigquery"
    )

    return df_keywords


@task
def extract_text_length_from_job_descriptions(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """ Extract text length from job descriptions """

    logger = get_run_logger()

    df["text_length_in_chars"] = df["description"].apply(lambda x: len(x))
    df["number_tokens"] = df["description"].apply(
        lambda x: len(process.tokenize_text(x))
    )

    logger.info("INFO level log message")
    logger.info(
        f"Created Text Length Dataframe with Shape: '{df.shape}' from Bigquery"
    )

    return df


@task
def identify_extension_type_from_extensions(
    df: pd.DataFrame, regex_pattern: re.Match
) -> pd.DataFrame:
    """ Identify extension type from extension (in Job description) """

    logger = get_run_logger()

    df["extension_type"] = df["extensions"].apply(
        lambda x: engineer.identify_extension_type(x, regex_pattern)
    )

    df = (
        df.set_index(["job_id"])
        .stack()
        .reset_index()
        .rename(columns={"level_1": "extension_type", 0: "extension_value"})
    )

    logger.info("INFO level log message")
    logger.info(
        f"Created Extension (Type) Dataframe with Shape: '{df.shape}' from Bigquery"
    )

    return df


@flow
def final_bigquery_flow(
    keyword_extract_config: KeywordExtractionConfig, regex_config: RegexConfig
):
    """ Create Final Bigquery Tables """

    # Load Credentials and Config
    gcp_credentials = GcpCredentials.load("gcp-credentials")

    # Load Job Results
    df = load_job_results(gcp_credentials)

    # Extract Keywords from Job Descriptions
    df_keywords = extract_keywords_from_job_descriptions(
        df[["job_id", "description"]], keyword_extract_config
    )

    # Extract Text Length from Job Descriptions
    df_text_length = extract_text_length_from_job_descriptions(
        df[["job_id", "description"]]
    )

    # Identify Extension Type from Extension
    df_extension_type = identify_extension_type_from_extensions(
        df[["job_id", "extensions"]], regex_config.extension_regex
    )

    df_job_results = pd.concat(
        [df, df_keywords, df_text_length, df_extension_type], axis=1
    )
    df_job_results.to_csv("data/final/job_results.csv", index=False)


if __name__ == "__main__":
    final_bigquery_flow(KeywordExtractionConfig(), RegexConfig())
