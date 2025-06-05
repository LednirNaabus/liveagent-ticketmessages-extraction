import os
import json
import argparse
import asyncio
import aiohttp
import pytz
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent_client import async_ping, async_agents, async_tickets, fetch_all_messages

manila_tz = pytz.timezone('Asia/Manila')

def parse_arguments():
    """
    Defines and parses the command-line arguments for the script. Use `-h` to output the help page.

    Returns:
        - `argparse.Namespace` - an object containing the values of the parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description="Fetch tickets and messages using LiveAgent API.")
    parser.add_argument(
        "--max_pages", "-mp",
        type=int,
        default=1,
        help="Max pages to fetch (default: 1, max page is '_page' in LiveAgent API)"
    )
    parser.add_argument(
        "--per_page", "-pp",
        type=int,
        default=10,
        help="Number of records to fetch per page (default: 10, per page is '_perPage' in LiveAgent API)"
    )
    parser.add_argument(
        "--skip_bq", "-sbq",
        action="store_true",
        help="Skip loading data to BigQuery"
    )
    parser.add_argument(
        "--ids", "-i",
        action="store_true",
        help="Only fetch ticket IDs and skip ticket messages"
    )
    parser.add_argument(
        "--date", "-d",
        type=str,
        help="[OPTIONAL] Use this only to fetch for a specific day (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--start_date", "-sd",
        type=str,
        help="[REQUIRED] Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end_date", "-ed",
        type=str,
        help="[REQUIRED] End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--weekly", "-w",
        action="store_true",
        help="Split into weekly"
    )
    parser.add_argument(
        "--csv", "-c",
        action="store_true",
        help="Store data into csv file"
    )
    return parser.parse_args()

def get_date(start_date, end_date, days=7):
    """
    Generates a list of date ranges (as tuples), which breaks a given date range into smaller chunks accessible as attributes.

    Parameters:
        - start_date (`datetime.date`) - the start of the overall date range
        - end_date (`datetime.date`) - the end of the overall date range
        - days (`int`) - the number of days in each chunk; default is 7

    Returns:
        `List[Tuple[datetime.date, datetime.date]]`:
        - a list of tuples, each containing a start and end date representing a chunk of the original date range
    """
    chunks = []
    current = start_date

    while current < end_date:
        next_date = min(current + timedelta(days=days-1), end_date)
        chunks.append((current, next_date))
        current = next_date + timedelta(days=1)

    return chunks

def set_date_filter(start_str: str):
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
    return json.dumps([
        ["date_changed", "D>=", f"{start_str}"]
        # ["date_created", "D<=", f"{end_str} 23:59:59"]
    ])

def set_timezone(df: pd.DataFrame, column: str, target_tz: str) -> pd.DataFrame:
    df[column] = pd.to_datetime(df[column], errors="coerce").dt.tz_localize('UTC')
    df[column] = df[column].apply(lambda x: x.astimezone(target_tz) if pd.notnull(x) else x)
    return df

def drop_cols(df: pd.DataFrame):
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

async def process_range(session, args, start_str: str, end_str: str):
    """
    Processes a range of dates by fetching ticket data from the API. It either fetches only ticket IDs
    or detailed messages depending on the command-line arguments provided when running the program. The output
    is saved to a CSV file and optionally uploaded to BigQuery with an auto-generated schema.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - args (`argparse.Namespace`) - the parsed command-line arguments containing options like `max_pages`, `ids`, or `skip_bq`
        - start_str (`start_str`) - the start date of the range to process in string format
        - end_str (`end_str`) - the end date of the range to process in string format
    
    Returns:
        None
    """
    config.ticket_payload["_filters"] = set_date_filter(start_str)
    agents_data = await async_agents(session)
    agent_lookup = dict(zip(agents_data["id"], agents_data["name"]))
    tickets_data = await async_tickets(session, agent_lookup, max_pages=args.max_pages)

    if args.ids:
        ticket_ids = {
            "ticket_id": [],
            "owner_contactid": [],
            "owner_email": [],
            "owner_name": [],
            "departmentid": [],
            "agentid": [],
            "agent_name": [],
            "status": [],
            "tags": [],
            "code": [],
            "channel_type": [],
            "date_created": [],
            "date_changed": [],
            "date_resolved": [],
            "date_due": [],
            "date_deleted": [],
            "last_activity": [],
            "last_activity_public": [],
            "public_access_urlcode": [],
            "subject": [],
            "custom_fields": [],
            "datetime_extracted": []
        }

        print(f"Saving ticket IDs from {start_str} to {end_str}...")
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i in tqdm(range(len(tickets_data["id"])), desc="Processing ticket IDs"):
            ticket_ids["ticket_id"].append(tickets_data["id"][i])
            ticket_ids["owner_contactid"].append(tickets_data["owner_contactid"][i])
            ticket_ids["owner_email"].append(tickets_data["owner_email"][i])
            ticket_ids["owner_name"].append(tickets_data["owner_name"][i])
            ticket_ids["departmentid"].append(tickets_data["departmentid"][i])
            ticket_ids["agentid"].append(tickets_data["agentid"][i])
            ticket_ids["agent_name"].append(tickets_data["agent_name"][i])
            ticket_ids["status"].append(tickets_data["status"][i])
            ticket_ids["tags"].append(','.join(tickets_data["tags"][i]))
            ticket_ids["code"].append(tickets_data["code"][i])
            ticket_ids["channel_type"].append(tickets_data["channel_type"][i])
            ticket_ids["date_created"].append(tickets_data["date_created"][i])
            ticket_ids["date_changed"].append(tickets_data["date_changed"][i])
            ticket_ids["date_resolved"].append(tickets_data["date_resolved"][i])
            ticket_ids["date_due"].append(tickets_data["date_due"][i])
            ticket_ids["date_deleted"].append(tickets_data["date_deleted"][i])
            ticket_ids["last_activity"].append(tickets_data["last_activity"][i])
            ticket_ids["last_activity_public"].append(tickets_data["last_activity_public"][i])
            ticket_ids["public_access_urlcode"].append(tickets_data["public_access_urlcode"][i])
            ticket_ids["subject"].append(tickets_data["subject"][i])
            ticket_ids["custom_fields"].append(tickets_data["custom_fields"][i])
            ticket_ids["datetime_extracted"].append(date_now)

        df = pd.DataFrame(ticket_ids)
        df = set_timezone(df, "date_created", manila_tz)
        # df = drop_cols(df)

        if args.csv:
            file_name = os.path.join("csv", f"ticket_ids_{start_str}_to_{end_str}.csv")
            df.to_csv(file_name, index=False)
            print(f"Saved ticket IDs to file: {file_name}")

        if not args.skip_bq:
            print("Generating schema and uploading to BigQuery...")
            schema = generate_schema(df)
            load_data_to_bq(
                df,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                config.BQ_TABLE_NAME,
                "WRITE_APPEND",
                schema=schema
            )
        return

    agents_data = await async_agents(session)
    agent_lookup = dict(zip(agents_data["id"], agents_data["name"]))

    df = await fetch_all_messages(tickets_data, agent_lookup, max_pages=args.max_pages)
    df = set_timezone(df, "datecreated", manila_tz)
    # df = set_timezone(df, "date_created", manila_tz)
    # df = drop_cols(df)

    if args.csv:
        file_name = os.path.join("csv", f"messages_{start_str}_to_{end_str}.csv")
        df.to_csv(file_name, index=False)
        print(f"Saved output to: {file_name}")
    
    if not args.skip_bq:
        print("Generating schema and uploading to BigQuery...")
        schema = generate_schema(df)
        load_data_to_bq(
            df,
            config.GCLOUD_PROJECT_ID,
            config.BQ_DATASET_NAME,
            config.BQ_TABLE_NAME,
            "WRITE_APPEND",
            schema=schema
        )

async def main():
    """
    Main entry point for the program.
    """
    args = parse_arguments()

    if not args.date and (not args.start_date or not args.end_date):
        print("Error: You must provide either --date or both --start_date and --end_date.")
        return

    config.ticket_payload["_page"] = args.max_pages
    config.ticket_payload["_perPage"] = args.per_page
    config.messages_payload["_perPage"] = args.per_page

    os.makedirs("csv", exist_ok=True)

    if args.date:
        start_date = end_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    async with aiohttp.ClientSession() as session:
        success, ping_response = await async_ping(session)
        if not success:
            print("Ping failed: ", ping_response)
            exit(1)
        
        print(f"Ping to {config.base_url} successful.")

        if args.weekly:
            chunks = get_date(start_date, end_date)
            for chunk_start, chunk_end in chunks:
                start_str = chunk_start.strftime("%Y-%m-%d")
                end_str = chunk_end.strftime("%Y-%m-%d")
                print(f"\nProcessing {start_str} to {end_str}...")
                await process_range(session, args, start_str, end_str)
        else:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            await process_range(session, args, start_str, end_str)

# if __name__ == "__main__":
asyncio.run(main())