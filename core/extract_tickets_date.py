import json
import aiohttp
import datetime
import pandas as pd
from config import config
from core.liveagent_client import async_agents, async_tickets_filtered, fetch_all_messages

def set_filter(start: str, end: str):
    return json.dumps([
        ["date_created", "D>=", f"{start} 00:00:00"],
        ["date_created", "D<=", f"{end} 23:59:59"]
    ])

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

async def process_tickets_day():
    config.ticket_payload["_page"] = 100
    config.ticket_payload["_perPage"] = 100
    config.messages_payload["_perPage"] = 100

    today_date = datetime.datetime.today().strftime("%Y-%m-%d")
    async with aiohttp.ClientSession() as session:
        agents = await async_agents(session)
        agents_lookup = dict(zip(agents["id"], agents["name"]))

        config.ticket_payload["_filters"] = set_filter(today_date, today_date)
        print(config.ticket_payload["_filters"])
        tickets = await async_tickets_filtered(session, config.ticket_payload, 1)

        messages_df = await fetch_all_messages(tickets, agents_lookup, 1)
        print(messages_df.keys())
        # columns to remove:
        # message_id, type, agent_id
        messages_df = drop_cols(messages_df)
        messages_df.to_csv("testing-test.csv", index=False)