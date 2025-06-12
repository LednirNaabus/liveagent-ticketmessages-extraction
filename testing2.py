import pytz
import asyncio
import aiohttp
import traceback
import pandas as pd
from datetime import datetime
from config import config
from core.liveagent_client import async_tickets
from utils.bq_utils import generate_schema, load_data_to_bq

def set_timezone(df: pd.DataFrame, *columns: str, target_tz: str, skip_localized: set = None) -> pd.DataFrame:
    skip_localized = skip_localized or set()
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
        if column not in skip_localized:
            df[column] = df[column].dt.tz_localize('UTC')
            df[column] = df[column].apply(
                lambda x: x.astimezone(target_tz).replace(tzinfo=None) if pd.notnull(x) else x
            )
    return df

def normalize_custom_fields(df: pd.DataFrame) -> pd.DataFrame:
    df["custom_fields"] = df["custom_fields"].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) == 1 and isinstance(x[0], dict) else None
    )
    return df

async def tickets():
    async with aiohttp.ClientSession() as session:
        try:
            date_extracted = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            tickets_df = await async_tickets(session, max_pages=1)
            tickets_df["datetime_extracted"] = date_extracted
            tickets_df = set_timezone(tickets_df, "datetime_extracted", "date_created", "date_changed", "date_resolved", "last_activity", "last_activity_public", skip_localized={"datetime_extracted"}, target_tz=pytz.timezone('Asia/Manila'))
            print("Generating schema and loading data to BigQuery...")
            tickets_df = normalize_custom_fields(tickets_df)
            schema = generate_schema(tickets_df)
            load_data_to_bq(
                tickets_df,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                "tickets_testing",
                "WRITE_TRUNCATE",
                schema
            )
        except Exception as e:
            print(f"Exception occured in 'tickets()': {e}")
            traceback.print_exc()
            pass

asyncio.run(tickets())