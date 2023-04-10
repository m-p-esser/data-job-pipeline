""" Flow and Tasks to engineer and extract features from raw Bigquery data and load them in final tables """

import json
import re

import pandas as pd
from google.cloud import bigquery
from prefect import flow, get_run_logger, task
from prefect_gcp.credentials import GcpCredentials

from etl import engineer, process
from utils.config import KeywordExtractionConfig, RegexConfig


@task
def load_job_results(gcp_credentials: GcpCredentials) -> pd.DataFrame:
    """ Load Job Results Dataframe from Bigquery"""

    logger = get_run_logger()

    query = """
    SELECT jr.*, sm.created_at, sm.google_jobs_url, sp.q
    FROM `raw.job_results` jr
    JOIN `raw.search_metadata` sm ON jr.search_id = sm.id
    JOIN `raw.search_parameters` sp ON jr.search_id = sp.search_id
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
def extract_htidocid(df: pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()

    df["htidocid"] = df["job_id"].apply(lambda x: json.loads(x)["htidocid"])

    logger.info("INFO level log message")
    logger.info(f"There are {df['job_id'].nunique()} unique Job IDs")
    logger.info(f"There are {df['htidocid'].nunique()} unique htidocids")

    logger.info(f"Created Dataframe with Shape: '{df.shape}' from Bigquery")

    return df


@task
def deduplicate_job_results(df: pd.DataFrame) -> pd.DataFrame:
    """ Deduplicate Job Results """

    logger = get_run_logger()

    # Convert to Datetime
    df["created_at"] = pd.to_datetime(df["created_at"])

    # Deduplicate
    df = df.sort_values(by=["created_at"])
    df = df.drop_duplicates(
        subset=["htidocid"], keep="first"
    )  # Keep latest Job Ad

    # Count duplicates
    number_duplicates = df.duplicated(subset=["htidocid"], keep="first").sum()

    logger.info("INFO level log message")
    logger.info(
        f"Deduplicated Job Results Dataframe with Shape: '{df.shape}' from Bigquery"
    )
    logger.info(f"Number of Duplicates: '{number_duplicates}'")

    return df


@task
def identify_extension_type_from_extensions(
    df: pd.DataFrame, regex_pattern: re.Match
) -> pd.DataFrame:
    """ Identify extension type from extension (in Job description) """

    logger = get_run_logger()

    # One Row per Extension (instead of List of Extensions)
    df = df.explode("extensions")

    # Identify Extension Type
    df["extension_type"] = df["extensions"].apply(
        lambda x: engineer.identify_extension_type(x, regex_pattern)
    )

    # Filter out other extension types (as they produce duplicates)
    df = df[df["extension_type"] != "other"]

    # Create new Dataframe with one column for each extension type
    df = (
        df[["job_id", "extensions", "extension_type"]]
        .set_index(["job_id", "extension_type"])
        .unstack()
        .reset_index()
    )
    df.columns = [col[1] if col[1] != "" else col[0] for col in df.columns]

    logger.info("INFO level log message")
    logger.info(
        f"Created Extension (Type) Dataframe with Shape: '{df.shape}' from Bigquery"
    )

    return df


@task
def calculate_posting_date(
    df: pd.DataFrame, pattern: re.Match
) -> pd.DataFrame:
    """ Calculate the day the Job was posted """

    # Extract Number and Unit of Time Period (e.g. 3 and Tage)
    df[["posted_n_periods_ago_number", "posted_n_periods_ago_unit"]] = df[
        "posted_n_periods_ago"
    ].str.extract(pattern)
    df["posted_n_periods_ago_number"] = df[
        "posted_n_periods_ago_number"
    ].astype("int32")

    # Replace Synonyms
    df["posted_n_periods_ago_unit"] = df["posted_n_periods_ago_unit"].map(
        {"Stunden": "Stunde", "Tagen": "Tage"}
    )

    # Normalize by converting Time Period to Hours
    df["posted_n_periods_ago_in_hours"] = df.apply(
        lambda row: row["posted_n_periods_ago_number"] * 24
        if row["posted_n_periods_ago_unit"] == "Tage"
        else row["posted_n_periods_ago_number"] * 1,
        axis=1,
    )

    # Convert to Datetime
    df["created_at"] = pd.to_datetime(df["created_at"])

    # Calculate Posting Date
    df["posted_at"] = df["created_at"] - pd.to_timedelta(
        df["posted_n_periods_ago_in_hours"], unit="h"
    )

    return df


@task
def construct_other_features(df: pd.DataFrame) -> pd.DataFrame:
    """ Creat other miscellaneous features """

    logger = get_run_logger()

    df["homeoffice_yes_no"] = df["description"].apply(
        lambda x: "yes" if "homeoffice" in x.lower() else "no"
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

    # Extract htidocid from Job ID
    df_htidocid = extract_htidocid(df)

    # Deduplicate Job Results
    df_deduplicated = deduplicate_job_results(df_htidocid)

    # Identify Extension Type from Extension
    df_extension_type = identify_extension_type_from_extensions(
        df_deduplicated[["job_id", "extensions"]], regex_config.day_hour_regex
    )

    # Calculate Posting Date

    ## Get Created at for each Job
    df_extension_type_with_created_at = pd.merge(
        df_extension_type,
        df_deduplicated[["job_id", "created_at"]],
        on="job_id",
        how="left",
    )

    ## Filter to only contain rows with "posted_n_periods_ago" value
    df_extension_type_filtered = df_extension_type_with_created_at.dropna(
        subset=["posted_n_periods_ago"]
    )

    ## Calculate Posting Date
    df_posting_date = calculate_posting_date(
        df_extension_type_filtered, regex_config.day_hour_regex
    )

    # Construct Other Features
    df_homeoffice = construct_other_features(df[["job_id", "description"]])

    # Combine Dataframes
    df_job_results = pd.concat(
        [
            df_deduplicated,
            df_keywords,
            df_text_length,
            df_extension_type,
            df_posting_date,
            df_homeoffice,
        ],
        axis=1,
    )

    # # Encode missing values correctly
    # df_job_results = df_job_results.replace({'-': np.nan, '[]': np.nan})

    # Remove Duplicate Columns
    df_job_results = df_job_results.loc[
        :, ~df_job_results.columns.duplicated()
    ].copy()

    # Save to CSV
    df_job_results.to_csv("data/final/job_results.csv", index=False, sep=";")

    # json_records = df.to_json(orient='records')

    # client = bigquery.Client()

    # job_config = bigquery.LoadJobConfig(
    #     autodetect=True
    # )

    # project_id = gcp_credentials.project
    # table_ref = f"{project_id}.final.job_results"

    # job = client.load_table_from_json(json_records, table_ref, job_config=job_config)

    # job.result()

    # schema = [
    #     bigquery.SchemaField(name="search_id", field_type="STRING", mode="REQUIRED")
    # ]

    # project_id = gcp_credentials.project
    # table_ref = f"{project_id}.final.job_results"

    # job_config = bigquery.LoadJobConfig(
    #     schema=schema
    # )

    # df = pd.DataFrame(df["search_id"])
    # job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # schema = [
    #     bigquery.SchemaField("search_id", type="STRING", mode="REQUIRED"),
    #     bigquery.SchemaField("datawarehousing", type="STRING", mode="NULLABLE", default_value_expression=())]

    # project_id = gcp_credentials.project
    # table_ref = f"{project_id}.final.job_results"

    # table = bigquery.Table(table_ref=table_ref,schema=schema)

    # client.create_table(table)

    # errors = client.insert_rows_from_dataframe(table, df_job_results[["search_id", "job_id"]])
    # for chunk in errors:
    #     print(f"encountered {len(chunk)} errors: {chunk}")

    # # writer = pyarrow.BufferOutputStream()
    # # pyarrow.parquet.write_table(
    # #     pyarrow.Table.from_pandas(df_job_results),
    # #     writer,
    # #     use_compliant_nested_type=True
    # # )
    # # reader = pyarrow.BufferReader(writer.getvalue())

    # # client = bigquery.Client()
    # # parquet_options = bigquery.format_options.ParquetOptions()
    # # parquet_options.enable_list_inference = True
    # # job_config = bigquery.LoadJobConfig()
    # # job_config.source_format = bigquery.SourceFormat.PARQUET
    # # job_config.parquet_options = parquet_options

    # # job = client.load_table_from_file(
    # #     reader, "final.job_results", job_config=job_config
    # # )

    # # for col in df_job_results.columns:
    # #     print(f"Saving column '{col}' to Bigquery")
    # #     try:
    # #         df = pd.DataFrame(df_job_results[col])
    # #         df.to_gbq(f"final.job_results_{col}", project_id=gcp_credentials.project, if_exists="replace")
    # #         print(f"Sucessfully saved column '{col}' to Bigquery")
    # #     except Exception as e:
    # #         print(f"Could not save column '{col}' to Bigquery: {e}")

    # # # Pandas to Bigquery
    # # df_job_results.to_gbq("final.job_results", project_id=gcp_credentials.project, if_exists="replace")


if __name__ == "__main__":
    final_bigquery_flow(KeywordExtractionConfig(), RegexConfig())
