import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField
from typing import List

from config import config

def get_client():
    return {
        'client': config.BQ_CLIENT,
        'credentials': config.creds,
        'project_id': config.creds['project_id']
    }

def ensure_dataset(project_id: str, dataset_name: str, client: bigquery.Client):
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-southeast1"
        client.create_dataset(dataset, timeout=30)
        print(f"Created dataset '{dataset_id}'")

def ensure_table(project_id: str, dataset_name: str, table_name: str, client: bigquery.Client, schema=None):
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema) if schema else bigquery.Table(table_id)
        client.create_table(table)
        print(f"Created table '{table_id}'")

def generate_schema(df: pd.DataFrame) -> List[SchemaField]:
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "DATETIME",
    }

    schema = []
    for column, dtype in df.dtypes.items():
        val = df[column].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"

        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_schema(pd.json_normalize(val))
        else:
            fields = ()
        
        # type = "RECORD" if fields else TYPE_MAPPING.get(dtype.kind)
        if fields:
            field_type = "RECORD"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "DATETIME"
        else:
            field_type = TYPE_MAPPING.get(dtype.kind, "STRING")
        schema.append(
            SchemaField(
                name=column,
                field_type=field_type,
                mode=mode,
                fields=fields,
            )
        )

    return schema

def load_data_to_bq(df: pd.DataFrame, project_id: str, dataset_name: str, table_name: str, write_mode: str="WRITE_APPEND", schema=None):
    client = get_client()['client']
    ensure_dataset(project_id, dataset_name, client)
    ensure_table(project_id, dataset_name, table_name, client, schema)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_mode,
        autodetect=schema is None,
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        table.expires = None
        client.update_table(table, ["expires"])
        print(f"Successfully loaded {df.shape[0]} rows into {table_id}")
        return f"Loaded {df.shape[0]} rows into {table_id}"
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")
        return f"Failed to upload data: {e}"