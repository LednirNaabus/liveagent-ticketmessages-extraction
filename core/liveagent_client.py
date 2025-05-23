import asyncio
import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from config import config

# For API rate limits
# From LiveAgent API Documentation:
# The API rate limit is set right now to 180 requests per minute, counted for each API key separately.
sem = asyncio.Semaphore(2) # 2 concurrent request muna at a time
THROTTLE_DELAY = 0.4 # for rate control; (180 requests/min = 1 request every ~0.33s)

async def async_ping(session) -> tuple[bool, dict]:
    try:
        async with session.get(f"{config.base_url}/ping") as response:
            status_ok = response.status == 200
            response_json = await response.json()
            return status_ok, response_json
    except aiohttp.ClientError as e:
        print(f"Ping failed: {e}")
        return False, {}

async def async_paginate(session, url: str, payload: dict, max_pages: int, headers: dict) -> list:
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

# Generic get list of tickets
async def fetch_tickets(session, payload: dict, max_pages: int = 5) -> dict:
    ticket_data = await async_paginate(
        session=session,
        url=config.tickets_list_url,
        payload=payload,
        max_pages=max_pages,
        headers=config.headers
    )

    field_keys = [
        'id', 'tags', 'code', 'owner_contactid', 'owner_email', 'owner_name',
        'date_created', 'agentid', 'subject', 'status', 'channel_type'
    ]

    tickets_dict = {
        "id": [],
        "tags": [],
        "code": [],
        "owner_contactid": [],
        "owner_email": [],
        "owner_name": [],
        "ticket_date_created": [],
        "agentid": [],
        "subject": [],
        "status": [],
        "channel_type": [],
    }

    for ticket in ticket_data:
        for key in field_keys:
            dest_key = "ticket_date_created" if key == "date_created" else key
            default_value = [] if key in ["tags", "code"] else None
            tickets_dict[dest_key].append(ticket.get(key, default_value))

    return tickets_dict

# Generic get list of tickets
async def async_tickets(session, max_pages: int = 5) -> dict:
    return await fetch_tickets(session, config.ticket_payload.copy(), max_pages)

# Fetch tickets but with custom payload
async def async_tickets_filtered(session, payload: dict, max_pages: int = 5) -> dict:
    return await fetch_tickets(session, payload, max_pages)

async def tickets_by_date(session, date_str: str, max_pages: int = 5) -> dict:
    payload = config.ticket_payload.copy()
    payload["date_created"] = date_str
    return await fetch_tickets(session, payload, max_pages)

async def async_agents(session, max_pages: int = 5) -> dict:
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

async def get_ticket_messages_for_one(session, ticket_id, ticket_date_created, code, owner_name, subject, agent_id, status, channel_type, tags, agent_lookup, max_pages):
    url = f"{config.tickets_list_url}/{ticket_id}/messages"
    payload = config.messages_payload.copy()
    
    messages_data = await async_paginate(
        session=session,
        url=url,
        payload=payload,
        headers=config.headers,
        max_pages=max_pages
    )

    ticket_messages = []
    for item in messages_data:
        messages = item.get("messages", [])
        for message in messages:
            msg_userid = message.get("userid")
            msg_type = message.get("type")

            sender = agent_lookup.get(msg_userid) if msg_userid in agent_lookup else owner_name

            if msg_userid in agent_lookup:
                receiver_type = "Customer"
                receiver_name = owner_name
            else:
                receiver_type = "Agent"
                receiver_name = agent_lookup.get(agent_id)

            ticket_messages.append({
                "ticket_id": ticket_id,
                "code": code,
                "owner_name": owner_name,
                "message_id": message.get("id"),
                "subject": subject,
                "message": message.get("message"),
                "datecreated": message.get("datecreated"),
                "ticket_date_created": ticket_date_created,
                "type": msg_type,
                "agentid": agent_id,
                "status": status,
                "channel_type": channel_type,
                "agent_name": agent_lookup.get(agent_id),
                "sender_name": sender,
                "receiver_type": receiver_type,
                "receiver_name": receiver_name,
                "tags": ','.join(tags) if tags else None
            })
    return ticket_messages

async def fetch_all_messages(response: dict, agent_lookup: dict, max_pages: int = 5) -> pd.DataFrame:
    ticket_ids = response.get("id", [])
    ticket_date_created = response.get("ticket_date_created", [])
    owner_names = response.get("owner_name", [])
    subjects = response.get("subject", [])
    agentids = response.get("agentid", [])
    tags_list = response.get("tags", [])
    code = response.get("code", [])
    status = response.get("status", [])
    channel_type = response.get("channel_type", [])

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, ticket_id in enumerate(ticket_ids):
            tasks.append(get_ticket_messages_for_one(
                session,
                ticket_id,
                ticket_date_created[i] if i < len(ticket_date_created) else None,
                code[i] if i < len(code) else None,
                owner_names[i] if i < len(owner_names) else None,
                subjects[i] if i < len(subjects) else None,
                agentids[i] if i < len(agentids) else None,
                status[i] if i < len(status) else None,
                channel_type[i] if i < len(channel_type) else None,
                tags_list[i] if i < len(tags_list) else None,
                agent_lookup,
                max_pages
            ))

        results = await tqdm_asyncio.gather(*tasks, desc="Fetching ticket messages")
    
    all_messages = [msg for sublist in results for msg in sublist]
    return pd.DataFrame(all_messages)