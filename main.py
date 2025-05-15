import os
import json
import argparse
import asyncio
import aiohttp
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent_client import async_ping, async_agents, async_tickets, fetch_all_messages

def parse_arguments():
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
        "--start_date", "-sd",
        type=str,
        help="[REQUIRED] Start date (YYYY-MM-DD)",
        required=True
    )
    parser.add_argument(
        "--end_date", "-ed",
        type=str,
        help="[REQUIRED] End date (YYYY-MM-DD)",
        required=True
    )
    parser.add_argument(
        "--weekly", "-w",
        action="store_true",
        help="Split into weekly"
    )
    return parser.parse_args()

def get_date(start_date, end_date, days=7):
    chunks = []
    current = start_date

    while current < end_date:
        next_date = min(current + timedelta(days=days-1), end_date)
        chunks.append((current, next_date))
        current = next_date + timedelta(days=1)

    return chunks

def set_date_filter(start_str: str, end_str: str):
    return json.dumps([
        ["date_created", "D>=", f"{start_str} 00:00:00"],
        ["date_created", "D<=", f"{end_str} 23:59:59"]
    ])

async def process_range(session, args, start_str: str, end_str: str):
    config.ticket_payload["_filters"] = set_date_filter(start_str, end_str)
    tickets_data = await async_tickets(session, max_pages=args.max_pages)

    if args.ids:
        ticket_ids = {
            "ticket_id": [],
            "code": [],
            "owner_name": [],
            "date_created": [],
        }

        print(f"Saving ticket IDs from {start_str} to {end_str}...")

        for i in tqdm(range(len(tickets_data["id"])), desc="Processing ticket IDs"):
            ticket_ids["ticket_id"].append(tickets_data["id"][i])
            ticket_ids["code"].append(tickets_data["code"][i])
            ticket_ids["owner_name"].append(tickets_data["owner_name"][i])
            ticket_ids["date_created"].append(tickets_data["date_created"][i])

        df = pd.DataFrame(ticket_ids)
        file_name = os.path.join("csv", f"ticket_ids_{start_str}_to_{end_str}.csv")
        df.to_csv(file_name, index=False)
        print(f"Saved ticket IDs to file: {file_name}")
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
    df["datecreated"] = pd.to_datetime(df["datecreated"], errors="coerce")

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
    args = parse_arguments()

    config.ticket_payload["_page"] = args.max_pages
    config.ticket_payload["_perPage"] = args.per_page
    config.messages_payload["_perPage"] = args.per_page

    os.makedirs("csv", exist_ok=True)

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
            await process_range(session, args, args.start_date, args.end_date)

if __name__ == "__main__":
    asyncio.run(main())