import aiohttp
import requests
import pandas as pd
from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent_client import fetch_tags

# async def extract_and_load_tags():
#     """
#     Calls `fetch_tags()` from `core.liveagent_client` and then loads
#     the data into BigQuery.
#     """
#     async with aiohttp.ClientSession() as session:
#         success, ping_response = await async_ping(session)
#         if not success:
#             print(f"Ping failed: {ping_response}")
#             exit(1)

#         print(f"Ping to {config.base_url} successful.")
#         print("Checkpoint")

#         # Stop
#         # tags = await fetch_tags(session)
#         try:
#             tags = await fetch_tags(session)
#         except Exception as e:
#             print("Error during fetch_tags():", str(e))
#             raise
#         print(tags)
#         df = pd.DataFrame(tags)
#         print(df)

#         print("Generating schema...")
#         schema = generate_schema(df)
#         print("Loading data into BigQuery...")
#         load_data_to_bq(
#             df,
#             config.GCLOUD_PROJECT_ID,
#             config.BQ_DATASET_NAME,
#             config.BQ_TABLE_NAME,
#             "WRITE_TRUNCATE",
#             schema
#         )
#         return tags
def extract_and_load_tags():
    """
    Calls `fetch_tags()` from `core.liveagent_client` and then loads
    the data into BigQuery.
    """
    tags_df = fetch_tags()
    print("Generating schema...")
    schema = generate_schema(tags_df)
    print("Loading data into BigQuery...")
    load_data_to_bq(
        tags_df,
        config.GCLOUD_PROJECT_ID,
        config.BQ_DATASET_NAME,
        config.BQ_TABLE_NAME,
        "WRITE_TRUNCATE",
        schema
    )
    return tags_df.to_dict(orient="records")