import json
import asyncio
import aiohttp
import argparse
import datetime
from config import config
from core.liveagent_client import async_agents, async_tickets_filtered, fetch_all_messages

def parse_arguments():
    parser = argparse.ArgumentParser(description="Fetch tickets and messages using LiveAgent API (one day).")
    parser.add_argument(
        "--date", "-d",
        type=str,
        help="The date you want to extract (YYYY-MM-DD)"
    )
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
    return parser.parse_args()

def set_filter(start: str, end: str):
    return json.dumps([
        ["date_created", "D>=", f"{start} 00:00:00"],
        ["date_created", "D<=", f"{end} 23:59:59"]
    ])

async def get_tickets_day():
    args = parse_arguments()
    config.ticket_payload["_page"] = args.max_pages
    config.ticket_payload["_perPage"] = args.per_page
    config.messages_payload["_perPage"] = args.per_page
    today_date = args.date

    # today_date = datetime.datetime.today().strftime("%Y-%m-%d")
    async with aiohttp.ClientSession() as session:
        agents = await async_agents(session)
        agents_lookup = dict(zip(agents["id"], agents["name"]))

        # ticket_payload = config.ticket_payload.copy()
        config.ticket_payload["_filters"] = set_filter(today_date, today_date)
        print(config.ticket_payload["_filters"])
        tickets = await async_tickets_filtered(session, config.ticket_payload, 1)
        print(tickets.keys())

        messages_df = await fetch_all_messages(tickets, agents_lookup, 1)
        print(messages_df.keys())
        # columns to remove:
        # message_id, type, agent_id
        messages_df.drop(columns=['message_id', 'type', 'agentid'], inplace=True)
        messages_df.to_csv("testing.csv", index=False)

asyncio.run(get_tickets_day())