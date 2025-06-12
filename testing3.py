import asyncio
import aiohttp
import traceback
import pandas as pd
from tqdm import tqdm
from config import config

sem = asyncio.Semaphore(2)
THROTTLE_DELAY = 0.4

async def async_paginate(session: aiohttp.ClientSession, url: str, payload: dict, max_pages: int, headers: dict) -> list:

    all_data = []
    page = 1

    while page <= max_pages:
        payload["_page"] = page
        async with sem:
            await asyncio.sleep(THROTTLE_DELAY)
            async with session.get(url, params=payload, headers=headers) as res:
                res.raise_for_status()
                data = await res.json()

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            break
        
        all_data.extend(data)
        page += 1

    return all_data

async def fetch_tickets(session: aiohttp.ClientSession, payload: dict, max_pages: int = 5) -> pd.DataFrame:
    try:
        ticket_data = await async_paginate(
            session=session,
            url=config.tickets_list_url,
            payload=payload,
            max_pages=max_pages,
            headers=config.headers
        )
        for ticket in ticket_data:
            ticket['tags'] = ','.join(ticket['tags']) if ticket.get('tags') else ''
        ticket_df = pd.DataFrame(ticket_data)
    except Exception as e:
        print(f"Exception occured in 'fetch_tickets()': {e}")
        traceback.format_exc()
    finally:
        return ticket_df

async def async_tickets(session: aiohttp.ClientSession, max_pages: int = 5) -> pd.DataFrame:
    return await fetch_tickets(session, config.ticket_payload.copy(), max_pages)

async def async_agents(session: aiohttp.ClientSession, max_pages: int = 5) -> dict:

    payload = {
        "_page": 1,
        "_perPage": 5
    }
    agents_data = await async_paginate(
        session=session,
        url=config.agents_list_url,
        payload=payload,
        max_pages=max_pages,
        headers=config.headers
    )
    agents_dict = {
        "id": [],
        "name": [],
        "email": [],
        "status": []
    }

    for agent in agents_data:
        agents_dict['id'].append(agent.get("id"))
        agents_dict['name'].append(agent.get("name"))
        agents_dict['email'].append(agent.get("email"))
        agents_dict['status'].append(agent.get("status"))

    return agents_dict

async def fetch_ticket_message(session: aiohttp.ClientSession, ticket_payload: dict, agent_dict: dict, max_pages: int = 5):
    # pass in /ticket to get ['ticket_id', 'owner_name']
    # do some processing to get 'agent_name'
    # Get /messages MessageGroup then MessageGroup[Messages]
    ticket_ids = ticket_payload.get("id", [])
    ticket_owner_names = ticket_payload.get("owner_name", [])
    ticket_message_all = {
        "ticket_id": [],
        "owner_name": [],
        "agent_id": [],
        "agent_name": [],
        "id": [],
        "parent_id": [],
        "userid": [],
        "user_full_name": [],
        "type": [],
        "status": [],
        "datecreated": [],
        "datefinished": [],
        "sort_order": [],
        "mail_msg_id": [],
        "pop3_msg_id": [],
        "message_id": [],
        "message_userid": [],
        "message_type": [],
        "message_datecreated": [],
        "message_format": [],
        "message": [],
        "message_visibility": [],
        "sender_name": [],
        "receiver_name": [],
        "sender_type": [],
        "receiver_type": []
    }

    try:
        for ticket_id, ticket_owner_name in tqdm(zip(ticket_ids, ticket_owner_names), total=len(ticket_ids), desc="Fetching ticket messages"):
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
                for message in message_group["messages"]:
                    msg_user_id = message.get("userid")
                    if msg_user_id in agent_dict:
                        sender_name = agent_dict[msg_user_id]
                        sender_type = "Agent"
                        receiver_name = ticket_owner_name
                        receiver_type = "Customer"
                        agent_id = msg_user_id
                    else:
                        sender_name = ticket_owner_name
                        sender_type = "Customer"
                        receiver_userid = message_group.get("userid")
                        if receiver_userid in agent_dict:
                            receiver_name = agent_dict[receiver_userid]
                        else:
                            receiver_name = "Unknown"
                        receiver_type = "Agent"
                        agent_id = receiver_userid
                    ticket_message_all["ticket_id"].append(ticket_id)
                    ticket_message_all["owner_name"].append(ticket_owner_name)
                    ticket_message_all["agent_id"].append(agent_id)
                    ticket_message_all["agent_name"].append(sender_name)
                    ticket_message_all["id"].append(message_group.get("id"))
                    ticket_message_all["parent_id"].append(message_group.get("parent_id"))
                    ticket_message_all["userid"].append(message_group.get("userid"))
                    ticket_message_all["user_full_name"].append(message_group.get("user_full_name"))
                    ticket_message_all["type"].append(message_group.get("type"))
                    ticket_message_all["status"].append(message_group.get("status"))
                    ticket_message_all["datecreated"].append(message_group.get("datecreated"))
                    ticket_message_all["datefinished"].append(message_group.get("datefinished"))
                    ticket_message_all["sort_order"].append(message_group.get("sort_order"))
                    ticket_message_all["mail_msg_id"].append(message_group.get("mail_msg_id"))
                    ticket_message_all["pop3_msg_id"].append(message_group.get("pop3_msg_id"))
                    ticket_message_all["message_id"].append(message.get("id"))
                    ticket_message_all["message_userid"].append(message.get("userid"))
                    ticket_message_all["message_type"].append(message.get("type"))
                    ticket_message_all["message_datecreated"].append(message.get("datecreated"))
                    ticket_message_all["message_format"].append(message.get("format"))
                    ticket_message_all["message"].append(message.get("message"))
                    ticket_message_all["message_visibility"].append(message.get("visibility"))
                    ticket_message_all["sender_name"].append(sender_name)
                    ticket_message_all["receiver_name"].append(receiver_name)
                    ticket_message_all["sender_type"].append(sender_type)
                    ticket_message_all["receiver_type"].append(receiver_type)
    except Exception as e:
        print(f"Exception occured in 'fetch_ticket_message()': {e}")
        traceback.format_exc()
        traceback.print_exc()
    finally:
        ticket_messages_df = pd.DataFrame(ticket_message_all)
        return ticket_messages_df

async def ticket_msgs():
    async with aiohttp.ClientSession() as session:
        try:
            tickets_df = await async_tickets(session, max_pages=1)
            agents_data = await async_agents(session, max_pages=100)
            agents_lookup = dict(zip(agents_data['id'], agents_data['name']))
            ticket_msg_df = await fetch_ticket_message(session, tickets_df, agents_lookup, max_pages=1)
            print(ticket_msg_df.head())
            ticket_msg_df.to_csv("s.csv", encoding="utf-8", index=False)
        except Exception as e:
            print(f"Exception occured: {e}")
            traceback.print_exc()

asyncio.run(ticket_msgs())