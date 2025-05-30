import pytz
import json
import aiohttp
import pandas as pd
from tqdm import tqdm
from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent_client import async_agents, async_tickets_filtered, fetch_all_messages, async_ping, async_tickets

def set_filter(date: pd.Timestamp):
    """
    Sets the filter of the API request, specifically the date range.

    Parameters:
        - start_str (`str`) - the starting date
        - end_str (`str`) - the ending date

    Returns:
        JSON:
            - A JSON string representing the date filter. The string is directly assigned to the
            `_filters` parameter in the API payload.
    """
    start = date - pd.Timedelta(hours=6)
    start = start.floor('h') # flatten the hour i.e. 06:00:00
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    return json.dumps([
        ["date_created", "D>=", f"{start}"],
        ["date_created", "D<=", f"{end}"]
    ])

def set_timezone(df: pd.DataFrame, *columns: str, target_tz: str) -> pd.DataFrame:
    """
    Sets the time zone of a selected DataFrame columns to target time zone.

    Parameters:
        - df (`pd.DataFrame`) - the DataFrame
        - *columns (`str`) - the column/(s) you want to change the time zone
        - target_tz (`str`) - the time zone you want to set

    Returns:
        pd.DataFrame:
            - A pandas DataFrame.
    """
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.tz_localize('UTC')
        df[column] = df[column].apply(lambda x: x.astimezone(target_tz) if pd.notnull(x) else x)
    return df

def format_date_col(df: pd.DataFrame, column: str, format: str = "%Y-%m-%d") -> pd.DataFrame:
    """
    Formats the selected date column to JSON serializable.

    Parameters:
        - df (`pd.DataFrame`) - DataFrame
        - column (`str`) - name of the pandas DataFrame column you want to format
        - format (`str`) - default: `"%Y-%m-%d"`, the date format
    
    Returns:
        pd.DataFrame:
            - Newly formatted pandas DataFrame.
    """
    df[column] = df[column].dt.strftime(format)
    return df

def drop_cols(df: pd.DataFrame) -> pd.DataFrame:
    try:
        cols_to_drop = ['message_id', 'type', 'agentid']
        existing = [col for col in cols_to_drop if col in df.columns]

        if existing:
            df.drop(columns=existing, axis=1, inplace=True)
        else:
            pass
    except Exception as e:
        print(df.columns)
        print(f"Exception: {e}")
    return df

async def extract_tickets(date: pd.Timestamp):
    config.ticket_payload["_page"] = 100
    config.ticket_payload["_filters"] = set_filter(date)

    async with aiohttp.ClientSession() as session:
        success, ping_response = await async_ping(session)
        if not success:
            print(f"Ping failed: {ping_response}")
            exit(1)

        print(f"Ping to {config.base_url} successful.")

        try:
            tickets_data = await async_tickets(session, max_pages=config.ticket_payload["_page"])
            print(config.ticket_payload["_filters"])
            print(config.ticket_payload["_page"])
            ticket_ids = {
                "ticket_id": [],
                "code": [],
                "owner_name": [],
                "date_created": [],
                "tags": []
            }

            for i in tqdm(range(len(tickets_data["id"])), desc="Processing ticket IDs"):
                ticket_ids["ticket_id"].append(tickets_data["id"][i])
                ticket_ids["code"].append(tickets_data["code"][i])
                ticket_ids["owner_name"].append(tickets_data["owner_name"][i])
                ticket_ids["date_created"].append(tickets_data["ticket_date_created"][i])
                ticket_ids["tags"].append(','.join(tickets_data["tags"][i]))

            tickets_df = pd.DataFrame(ticket_ids)
            tickets_df = set_timezone(tickets_df, "date_created", target_tz=pytz.timezone('Asia/Manila'))
            tickets_df = drop_cols(tickets_df)

            # load to BQ
            print("Generating schema...")
            schema = generate_schema(tickets_df)
            print("Loading data into BigQuery...")
            load_data_to_bq(
                tickets_df,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                "tickets_test",
                "WRITE_TRUNCATE",
                schema
            )

            # process datetime kasi maarte si JSON
            tickets_df = format_date_col(tickets_df, "date_created")
            return tickets_df.to_dict(orient="records")
        except Exception as e:
            print(f"Exception occurred in extract_tickets: {e}")

async def extract_ticket_messages():
    config.ticket_payload["_page"] = 100
    config.ticket_payload["_perPage"] = 100
    config.messages_payload["_perPage"] = 100

    today_date = pd.Timestamp.now().tz_localize('Asia/Manila')
    async with aiohttp.ClientSession() as session:
        success, ping_response = await async_ping(session)
        if not success:
            print(f"Ping failed: {ping_response}")
            exit(1)

        print(f"Ping to {config.base_url} successful.")
        try:
            agents = await async_agents(session)
            agents_lookup = dict(zip(agents["id"], agents["name"]))

            config.ticket_payload["_filters"] = set_filter(today_date)
            print(config.ticket_payload["_filters"])
            print("Extracting messages, this may take a while...")
            tickets = await async_tickets_filtered(session, config.ticket_payload, config.ticket_payload["_page"])

            messages_df = await fetch_all_messages(tickets, agents_lookup, 100)
            messages_df = drop_cols(messages_df)
            messages_df = set_timezone(messages_df, "datecreated", "ticket_date_created", target_tz=pytz.timezone('Asia/Manila'))

            print("Generating schema...")
            schema = generate_schema(messages_df)
            print("Loading data into BigQuery...")
            load_data_to_bq(
                messages_df,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                "messages_test",
                "WRITE_APPEND",
                schema
            )

            # process datetime kasi maarte si JSON
            messages_df = format_date_col(messages_df, "datecreated")
            messages_df = format_date_col(messages_df, "ticket_date_created")
            return messages_df.to_dict(orient="records")
        except Exception as e:
            print(f"Exception occured: {str(e)}")