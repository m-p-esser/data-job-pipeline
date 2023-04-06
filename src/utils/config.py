""" Create Pydantic Configuration models """

import itertools
import re

from prefect.blocks.system import Secret
from pydantic import BaseModel


class GoogleJobsAPIRequestParams(BaseModel):
    """Parameters for requesting the Google Jobs API endpoint from Serpapi"""

    engine: str = "google_jobs"
    q: str = "Data Analyst"
    hl: str = "de"
    gl: str = "de"
    lr: int = 25  # Search radius in km
    start: int = 0  # Offset, 10 = first ten search results are skipped
    google_domain: str = "google.de"
    location: str = "Cologne,North Rhine-Westphalia,Germany"
    api_key: str = Secret.load("serpapi-api-key").get()


class GoogleJobsAPIRequestConfig(BaseModel):
    """Configuration for requesting the Google Jobs API endpoint from Serpapi"""

    params: dict = dict(GoogleJobsAPIRequestParams())
    save_dir: str = "data/raw"
    file_name: str = "google_jobs"
    extension: str = "json"
    save_location: str = "gcs"
    dataset_id: str = "raw"  # BigQuery Dataset ID
    table_name: str = "google_jobs"  # BigQuery Table Name


class GCSFileSplittingConfig(BaseModel):
    """Configuration for splitting Raw GCS Files into seperate Files """

    load_dir: str = "data/raw/successful"
    save_dir: str = "data/processed"


class GCSToBigQueryConfig(BaseModel):
    """Configuration for storing GCS Files into Bigquery"""

    load_dir: str = "data/processed"
    dataset_id: str = "raw"  # BigQuery Dataset ID


class GoogleJobsAPIQueryCombinations(BaseModel):
    """Search Queries for requesting the Google Jobs API endpoint from Serpapi"""

    jobs: list[str] = ["Data Analyst", "Data Scientist", "Data Engineer"]
    locations: list[str] = [
        # Cities / regions nearby
        "Cologne,North Rhine-Westphalia,Germany",
        "Aachen,North Rhine-Westphalia,Germany",
        "Bonn,North Rhine-Westphalia,Germany",
        "Dusseldorf,North Rhine-Westphalia,Germany",
        # Other Cities / regions in North Rhine-Westphalia
        # "Essen,North Rhine-Westphalia,Germany",
        # "Dortmund,North Rhine-Westphalia,Germany",
        # "Duisburg,North Rhine-Westphalia,Germany",
        # "Bielfeld,North Rhine-Westphalia,Germany",
        # # Other big cities in Germany
        # "Berlin,Germany",
        # "Hamburg,Germany",
        # "Munich,Bavaria,Germany",
        # "Frankfurt,Hesse,Germany",
        # "Stuttgart,Baden-Wurttemberg,Germany",
        # "Leipzig,Saxony,Germany",
        # "Bremen,Bremen,Germany",
        # "Dresden,Saxony,Germany",
        # "Hannover,Lower Saxony,Germany",
        # "Nuremberg,Bavaria,Germany",
        # "Bochum,North Rhine-Westphalia,Germany",
        # "Wuppertal,North Rhine-Westphalia,Germany",
        # "Munster,North Rhine-Westphalia,Germany"
    ]
    start_offsets: list[int] = [0, 10, 20]  # Top 30 Results


def create_permutations(
    jobs: list, locations: list, start_offsets: list
) -> list[tuple]:
    """Create all possible combinations of the given parameters"""

    list_of_lists = [jobs, locations, start_offsets]
    permutations = list(itertools.product(*list_of_lists))
    return permutations


class KeywordExtractionConfig(BaseModel):
    """ Keywords to match against Job description """

    programming_markup_languages: list[str] = [
        "SQL",
        "C++",
        "C#",
        "HTML",
        "CSS",
        "C",
        "Dart",
        "Haskell",
        "Cobol",
        "Ruby",
        "Assembly",
        "Scala",
        "Lisp",
        "Python",
        "R",
        "Java",
        "Javascript",
        "Typescript",
        "Rust",
        "Go",
        "Elixir",
        "Clojure",
        "Julia",
        "Kotlin",
        "Swift",
        "PHP",
    ]
    command_line_tools: list[str] = ["Bash", "Shell", "Powershell"]
    databases: list[str] = [
        "MySQL",
        "PostgreSQL",
        "MongoDB",
        "Redis",
        "Cassandra",
        "Elasticsearch",
        "Oracle",
        "SQLite",
        "MariaDB",
        "CouchDB",
        "Neo4j",
        "Couchbase",
        "Memcached",
        "MSSQL",
    ]
    hosting_platforms: list[str] = [
        "AWS",
        "Azure",
        "GCP",
        "Heroku",
        "DigitalOcean",
        "Linode",
        "Firebase",
        "Cloudflare",
        "VMWare",
    ]
    data_orchestration_frameworks: list[str] = [
        "Airflow",
        "Prefect",
        "Dagster",
    ]
    neural_net_frameworks: list[str] = [
        "Keras",
        "Tensorflow",
        "Pytorch",
        "MXNet",
        "CNTK",
        "Theano",
        "Caffe",
    ]
    nlp_frameworks: list[str] = [
        "Huggingface",
        "Spacy",
        "NLTK",
        "Gensim",
        "StanfordNLP",
        "AllenNLP",
    ]
    data_processing_frameworks: list[str] = ["Spark", "Hadoop", "Dask", "Hive"]
    business_intelligence_tools: list[str] = [
        "Tableau",
        "PowerBI",
        "Microstrategy",
        "Qlik",
        "Metabase",
        "Domo",
    ]  # Add more
    devops_tools: list[str] = ["Docker", "Kubernetes", "Terraform"]
    version_control_tools: list[str] = [
        "Git",
        "Mercurial",
        "Subversion",
        "Perforce",
        "CVS",
    ]


class RegexConfig(BaseModel):
    """ Collection of Regex Patterns """

    extension_regex: re.Pattern = re.compile(
        r"vor\s+(\d+)\s+(Stunden|Tagen)"
    )  # to identify extension in job description
