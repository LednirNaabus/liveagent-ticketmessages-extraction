import asyncio
import aiohttp
import requests
import traceback
import pandas as pd
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from config import config

# For API rate limits
# From LiveAgent API Documentation:
# The API rate limit is set right now to 180 requests per minute, counted for each API key separately.
sem = asyncio.Semaphore(2) # 2 concurrent request muna at a time
THROTTLE_DELAY = 0.4 # for rate control; (180 requests/min = 1 request every ~0.33s)

async def async_ping(session: aiohttp.ClientSession) -> tuple[bool, dict]:
    """
    Checks if LiveAgent API is responding. See: [LiveAgent API](https://mechanigo.ladesk.com/docs/api/v3/#/ping/ping) for reference.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session

    Returns:
        tuple[bool, dict]:
            - The status code and JSON response if the API responds accordingly.
            - Otherwise, returns a Boolean False and an empty dictionary.
    """
    try:
        async with session.get(f"{config.base_url}/ping") as response:
            status_ok = response.status == 200
            response_json = await response.json()
            return status_ok, response_json
    except aiohttp.ClientError as e:
        print(f"Ping failed: {e}")
        return False, {}

async def async_paginate(session: aiohttp.ClientSession, url: str, payload: dict, max_pages: int, headers: dict) -> list:
    """
    Accepts a max number of pages and loops through until it reaches the last page. Utilizes `asyncio.sleep()` and `asyncio.Semaphore()`
    which helps make concurrent requests at a time (for rate limiting issues).

    **Note**: According to LiveAgent API, the API rate limit is 180 requests per minute, counted for each API key separately.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - url (`str`) - the API url
        - payload (`str`) - expects a dictionary; the params accepted by the API endpoint
        - max_pages (`int`) - the max number of pages you want to paginate through
        - headers (`dict`) - the header of the request to the API

    Returns:
        list:
            - A list of data fetched from the LiveAgent API.
    """
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

# async def fetch_tickets(session: aiohttp.ClientSession, payload: dict, max_pages: int = 5) -> dict:
#     """
#     The function that interacts with the `/tickets` endpoint of the LiveAgent API. Uses `async_paginate()`
#     to loop through a certain number of pages and stores the data in a dictionary.

#     Parameters:
#         - session (`aiohttp.ClientSession`) - the client session
#         - payload (`dict`) - dictionary of parameters to send with the request for filtering or modifying the ticket query
#         - max_pages (`int`) - maximum number of pages to retrieve; default is 5
#     """
#     ticket_data = await async_paginate(
#         session=session,
#         url=config.tickets_list_url,
#         payload=payload,
#         max_pages=max_pages,
#         headers=config.headers
#     )

#     field_keys = [
#         'id', 'tags', 'code', 'owner_contactid', 'owner_email', 'owner_name',
#         'date_created', 'agentid', 'subject', 'status', 'channel_type'
#     ]

#     tickets_dict = {
#         "id": [],
#         "tags": [],
#         "code": [],
#         "owner_contactid": [],
#         "owner_email": [],
#         "owner_name": [],
#         "ticket_date_created": [],
#         "agentid": [],
#         "subject": [],
#         "status": [],
#         "channel_type": [],
#     }

#     for ticket in ticket_data:
#         for key in field_keys:
#             dest_key = "ticket_date_created" if key == "date_created" else key
#             default_value = [] if key in ["tags", "code"] else None
#             tickets_dict[dest_key].append(ticket.get(key, default_value))

#     return tickets_dict
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
    """
    Fetches tickets using a **default** payload configuration defined in `config.ticket_payload`.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - max_pages (`int`) - the maximum number of pages to retrieve; default is 5

    Returns:
        dict:
            - A dictionary containing list of extracted ticket fields
    """
    return await fetch_tickets(session, config.ticket_payload.copy(), max_pages)

async def async_tickets_filtered(session: aiohttp.ClientSession, payload: dict, max_pages: int = 5) -> dict:
    """
    Fetches tickets with a **user-provided** payload for custom filtering.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - payload (`dict`) - custom parameters for fetching tickets
        - max_pages (`int`) - maximum number of pages to retrieve; default is 5

    Returns:
        dict:
            - a dictionary containing list of extracted ticket fields
    """
    return await fetch_tickets(session, payload, max_pages)

async def tickets_by_date(session: aiohttp.ClientSession, date_str: str, max_pages: int = 5) -> dict:
    """
    Fetches tickets filtered by a specific creation date.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - date_str (`str`) - the date string to filter tickets by (format: `YYYY-MM-DD`)
        - max_pages (`int`) - maximum number of pages to retrieve; default is 5

    Returns:
        dict:
            - dictionary containing list of extracted ticket fields
    """
    payload = config.ticket_payload.copy()
    payload["date_created"] = date_str
    return await fetch_tickets(session, payload, max_pages)

async def async_agents(session: aiohttp.ClientSession, max_pages: int = 5) -> dict:
    """
    Interacts with the `/agents` endpoint from the LiveAgent API to cross reference agent IDs. Gathers the
    agent ID, name, email, and status then stores them in a dictionary.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session
        - max_pages (`int`) - maximum number of pages to retrieve; default is 5

    Returns:
        dict:
            - a dictionary of ID, name, email and status for each agent
    """
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

# async def get_ticket_messages_for_one(session: aiohttp.ClientSession, ticket_id: str, ticket_date_created: str, code: str, owner_name: str, subject: str, agent_id: str, status: str, channel_type: str, tags: str, agent_lookup: str, max_pages: int = 5) -> list:
#     """
#     Interacts with the `/ticket/{ticket_id}/messages` endpoint of the LiveAgent API. It loops through
#     each page for the tickets and extracts the ticket's messages.

#     Parameters:
#         - session (`aiohttp.ClientSession`) - the client session
#         - ticket_id (`str`) - the unique ticket ID
#         - ticket_date_created (`str`) - date creation of the ticket
#         - code (`str`) - code for the ticket
#         - owner_name (`str`) - the owner of the ticket
#         - subject (`str`) - the subject of the ticket
#         - agent_id (`str`) - the agent ID for the ticket
#         - status (`str`) - whether or not the ticket has been resolved
#         - channel_type (`str`) - the ticket channel type
#         - agent_lookup (`str`) - used to cross reference the agent ID
#         - max_pages (`int`) - maximum number of pages to retrieve; default is 5

#     Returns:
#         list:
#             - list of ticket messages
#     """
#     url = f"{config.tickets_list_url}/{ticket_id}/messages"
#     payload = config.messages_payload.copy()
    
#     messages_data = await async_paginate(
#         session=session,
#         url=url,
#         payload=payload,
#         headers=config.headers,
#         max_pages=max_pages
#     )

#     ticket_messages = []
#     for item in messages_data:
#         messages = item.get("messages", [])
#         for message in messages:
#             msg_userid = message.get("userid")
#             msg_type = message.get("type")

#             sender = agent_lookup.get(msg_userid) if msg_userid in agent_lookup else owner_name

#             if msg_userid in agent_lookup:
#                 receiver_type = "Customer"
#                 receiver_name = owner_name
#             else:
#                 receiver_type = "Agent"
#                 receiver_name = agent_lookup.get(agent_id)

#             ticket_messages.append({
#                 "ticket_id": ticket_id,
#                 "code": code,
#                 "owner_name": owner_name,
#                 "message_id": message.get("id"),
#                 "subject": subject,
#                 "message": message.get("message"),
#                 "datecreated": message.get("datecreated"),
#                 "ticket_date_created": ticket_date_created,
#                 "type": msg_type,
#                 "agentid": agent_id,
#                 "status": status,
#                 "channel_type": channel_type,
#                 "agent_name": agent_lookup.get(agent_id),
#                 "sender_name": sender,
#                 "receiver_type": receiver_type,
#                 "receiver_name": receiver_name,
#                 "tags": ','.join(tags) if tags else None
#             })
#     return ticket_messages
async def get_ticket_messages_for_one(session: aiohttp.ClientSession, ticket_id: str, max_pages: int = 5) -> list:
    url = f"{config.tickets_list_url}/{ticket_id}/messages"
    payload = config.messages_payload.copy()
    
    messages_data = await async_paginate(
        session=session,
        url=url,
        payload=payload,
        headers=config.headers,
        max_pages=max_pages
    )
    # Get ticket_id, owner_name, agent_name
    # Get /messages MessageGroup
    # Get /messages MessageGroup[Messages]
    return messages_data
    # messages = []
    # for message in messages_data:
    #     for msg in message['messages']:
    #         print(msg)
    #         messages.append(msg)
    # return messages
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
        "message_visibility": []
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
                    ticket_message_all["ticket_id"].append(ticket_id)
                    ticket_message_all["owner_name"].append(ticket_owner_name)
                    ticket_message_all["agent_id"].append(agent_dict.get('name'))
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
                    # Do agent lookup here + sender type/name + receiver type/name
    except Exception as e:
        print(f"Exception occured in 'fetch_ticket_message()': {e}")
        traceback.format_exc()
    finally:
        ticket_messages_df = pd.DataFrame(ticket_message_all)
        return ticket_messages_df

# async def fetch_all_messages(response: dict, agent_lookup: dict, max_pages: int = 5) -> pd.DataFrame:
#     """
#     Fetches all messages for each ticket ID.

#     Parameters:
#         - response (`dict`) - expects the data from `/tickets` endpoint.
#         - agent_lookup (`dict`) - used to cross reference the agent ID
#         - max_pages (`int`) - maximum number of pages to retrieve; default is 5

#     Returns:
#         pd.DataFrame:
#             - a DataFrame of all messages for the ticket
#     """
#     ticket_ids = response.get("id", [])
#     ticket_date_created = response.get("ticket_date_created", [])
#     owner_names = response.get("owner_name", [])
#     subjects = response.get("subject", [])
#     agentids = response.get("agentid", [])
#     tags_list = response.get("tags", [])
#     code = response.get("code", [])
#     status = response.get("status", [])
#     channel_type = response.get("channel_type", [])

#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for i, ticket_id in enumerate(ticket_ids):
#             tasks.append(get_ticket_messages_for_one(
#                 session,
#                 ticket_id,
#                 ticket_date_created[i] if i < len(ticket_date_created) else None,
#                 code[i] if i < len(code) else None,
#                 owner_names[i] if i < len(owner_names) else None,
#                 subjects[i] if i < len(subjects) else None,
#                 agentids[i] if i < len(agentids) else None,
#                 status[i] if i < len(status) else None,
#                 channel_type[i] if i < len(channel_type) else None,
#                 tags_list[i] if i < len(tags_list) else None,
#                 agent_lookup,
#                 max_pages
#             ))

#         results = await tqdm_asyncio.gather(*tasks, desc="Fetching ticket messages")
    
#     all_messages = [msg for sublist in results for msg in sublist]
#     return pd.DataFrame(all_messages)
# async def fetch_all_messages(response: dict, agent_lookup: dict, max_pages: int = 5) -> pd.DataFrame:
#     pass

async def fetch_tags(session: aiohttp.ClientSession) -> pd.DataFrame:
    """
    Fetches all tags from LiveAgent API.

    Parameters:
        - session (`aiohttp.ClientSession`) - the client session

    Returns:
        pd.DataFrame:
            - a DataFrame of all tags
    """
    async with session.get(
        url=f"{config.base_url}/tags",
        headers=config.headers
    ) as res:
        res.raise_for_status()
        data = await res.json()

    try:
        df = pd.DataFrame(data=data)
        return df
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

async def fetch_users(session: aiohttp.ClientSession, user_id: str) -> pd.DataFrame:
    async with session.get(
        url=f"{config.base_url}/users/{user_id}",
        headers=config.headers
    ) as res:
        res.raise_for_status()
        data = await res.json()

    try:
        df = pd.DataFrame(data=data)
        return df
    except Exception as e:
        print(f"Exception occured while fetching users: {e}")
        raise