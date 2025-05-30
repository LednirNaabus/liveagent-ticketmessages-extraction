import aiohttp
import requests
import pandas as pd
from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent_client import async_ping, fetch_tags

async def extract_and_load_tags():
    """
    Calls `fetch_tags()` from `core.liveagent_client` and then loads
    the data into BigQuery.
    """
    async with aiohttp.ClientSession() as session:
        success, ping_response = await async_ping(session)
        if not success:
            print(f"Ping failed: {ping_response}")
            exit(1)

        print(f"Ping to {config.base_url} successful.")

        try:
            tags = await fetch_tags(session)
            print("Generating schema...")
            schema = generate_schema(tags)
            print("Loading data into BigQuery...")
            load_data_to_bq(
                tags,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                config.TAGS_TABLE,
                "WRITE_TRUNCATE",
                schema
            )
            return tags.to_dict(orient="records") # make object JSON serializable
        except Exception as e:
            print("Error during fetch_tags():", str(e))
            raise