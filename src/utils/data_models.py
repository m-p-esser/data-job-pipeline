""" Create Data models """

from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery


@dataclass
class BigQueryField:
    """BigQuery Field"""

    name: str
    type: str
    mode: str
    default_value_expression: Optional[str] = None

@dataclass
class BigQuerySchema:
    """BigQuery Schema"""

    dataset_id: str
    table_name: str
    fields: Optional[list[BigQueryField]] = None

    def format_schema(self):
        """Format BigQuery Schema using bigquery.SchemaField"""
        formatted_schema = [
            bigquery.SchemaField(field.name, field.type, field.mode, field.default_value_expression)
            for field in self.fields
        ]
        return formatted_schema
