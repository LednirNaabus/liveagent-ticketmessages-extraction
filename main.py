import argparse
from core.liveagent_client import tickets, get_ticket_messages, ping
from utils.bq_utils import load_data_to_bq, generate_schema
from config import config

def parse_arguments():
    parser = argparse.ArgumentParser(description="Fetch tickets and messages using LiveAgent API.")
    parser.add_argument(
        "--max_pages",
        type=int,
        default=1,
        help="Max pages to fetch (default: 1, max page is '_page' in LiveAgent API)"
    )
    parser.add_argument(
        "--per_page",
        type=int,
        default=10,
        help="Number of records to fetch per page (default: 10, per page is '_perPage' in LiveAgent API)"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    success, res = ping()
    if success:
        config.ticket_payload["_page"] = args.max_pages
        config.ticket_payload["_perPage"] = args.per_page
        config.messages_payload["_perPage"] = args.per_page

        all_tickets = tickets(max_pages=args.max_pages)
        df = get_ticket_messages(all_tickets, max_pages=args.max_pages)
        print(df)
        file_name = f"out-{config.filters[25:35]}.csv"
        df.to_csv(file_name, index=False)
        schema = generate_schema(df)
        print("Loading data to BigQuery...")
        load_data_to_bq(df, config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, config.BQ_TABLE_NAME, "WRITE_APPEND", schema=schema)
    else:
        print("Error API:")
        print(res)