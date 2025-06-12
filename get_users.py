import pytz
import json
import asyncio
import aiohttp
import pandas as pd
import traceback
from tqdm import tqdm
from datetime import datetime
from config import config
from core.liveagent_client import async_ping, async_paginate, async_agents, fetch_users, async_tickets_filtered
from utils.bq_utils import generate_schema, load_data_to_bq, sql_query_bq

async def get_agents():
    async with aiohttp.ClientSession() as session:
        success, ping_response = await async_ping(session)
        try:
            if success:
                print(f"Ping to {config.base_url}, successful.")
                all_agents = await async_agents(session, max_pages=1)
                df = pd.DataFrame(all_agents)
                print(df.head())
                df.to_csv("agents.csv", index=False)
                print("Generating schema and loading data to BigQuery...")
                schema = generate_schema(df)
                load_data_to_bq(
                    df,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    "agents",
                    "WRITE_TRUNCATE",
                    schema
                )
        except Exception as e:
            print(f"Exception occured in 'get_users()': {e}")
            print(ping_response)
            traceback.print_exc()

def set_date_filter(date: pd.Timestamp):
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    return json.dumps([
        ["date_created", "D>=", f"{start}"],
        ["date_created", "D<=", f"{end}"]
    ])

async def fetch_ticket_messages_userids(session, ticket_payload, max_pages = 5):
    ticket_ids = ticket_payload.get("id", [])
    all = {
        "ticket_id": [],
        "id": [],
        "userid": [],
        "datecreated": []
    }
    try:
        for ticket_id in tqdm(ticket_ids, total=len(ticket_ids), desc="Fetching Ticket User IDs"):
            ticket_messages_url = f"{config.tickets_list_url}/{ticket_id}/messages"
            ticket_messages_payload = config.messages_payload.copy()

            data = await async_paginate(
                session=session,
                url=ticket_messages_url,
                payload=ticket_messages_payload,
                headers=config.headers,
                max_pages=max_pages
            )
            for message_group in data:
                userid = message_group.get("userid")
                id = message_group.get("id")
                all["ticket_id"].append(ticket_id)
                all["id"].append(id)
                all["userid"].append(userid)
                all["datecreated"].append(message_group.get("datecreated"))
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        df = pd.DataFrame(all)
        return df

# CONCURRENCY_LIMIT = 5
# async def fetch_users_with_semaphore(session, sem, user_id):
#     async with sem:
#         try:
#             return await fetch_users(session, user_id)
#         except Exception as e:
#             print(e)
#             return None
# async def get_users():
#     semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
#     try:
#         query = f"SELECT DISTINCT(userid) FROM {config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.userids_temp"
#         res = sql_query_bq(query)
#         user_ids = list(res['userid'])
#         # print(len(user_ids))
#         async with aiohttp.ClientSession() as session:
#             tasks = [
#                 fetch_users_with_semaphore(session, semaphore, user_id)
#                 for user_id in tqdm(user_ids, desc="Queueing fetch tasks")
#             ]
#             users = []
#             for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching users"):
#                 result = await future
#                 if result is not None and len(result) > 0:
#                     users.append(result)

#         if users is not None and len(users) > 0:
#             print(users[0])
#             print("Generating schema and loading to BigQuery...")
#     except Exception as e:
#         print(e)
#         traceback.print_exc()

async def get_users():
    async with aiohttp.ClientSession() as session:
        q = f"SELECT DISTINCT(userid) FROM {config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.userids_temp"
        unique_userids = sql_query_bq(q)
        users = []
        try:
            for userid in tqdm(unique_userids['userid'], total=len(unique_userids['userid']), desc="Fetching user IDs"):
                user = await fetch_users(session, userid)
                users.append(user)
                await asyncio.sleep(0.25)
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            result_df = pd.concat(users, ignore_index=True)
            schema = generate_schema(result_df)
            load_data_to_bq(
                result_df,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                "users",
                "WRITE_APPEND",
                schema
            )

async def get_userids():
    async with aiohttp.ClientSession() as session:
        try:
            # now = pd.Timestamp.now(tz="UTC").astimezone(pytz.timezone('Asia/Manila'))
            # date = now - pd.Timedelta(hours=6)
            payload = {
                "_perPage": 100,
                "_filters": config.filters
            }
            tix = await async_tickets_filtered(session, payload, 100)
            userids = await fetch_ticket_messages_userids(session, tix, 100)
            userids["date_extracted"] = pd.to_datetime(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), errors="coerce")
            print("Generating schema and loading to BigQuery...")
            schema = generate_schema(userids)
            load_data_to_bq(
                userids,
                config.GCLOUD_PROJECT_ID,
                config.BQ_DATASET_NAME,
                "userids_temp",
                "WRITE_APPEND",
                schema
            )
        except Exception as e:
            print(e)

asyncio.run(get_users())