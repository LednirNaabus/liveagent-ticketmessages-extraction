import requests
import pandas as pd
from tqdm import tqdm
from config import config

def ping() -> bool:
    response = requests.get(
        url=f"{config.base_url}/ping"
    )
    return response.status_code == 200, response

def paginate(url: str, payload: dict, max_pages: int, headers: dict) -> list:
    all_data = []
    page = 1
    
    while page <= max_pages:
        payload["_page"] = page

        response = requests.get(url=url, params=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            break

        all_data.extend(data)
        page += 1

    return all_data

def tickets(max_pages: int = 5) -> dict:

    ticket_data = paginate(
        url=config.tickets_list_url,
        payload=config.ticket_payload.copy(),
        max_pages=max_pages,
        headers=config.headers
    )

    tickets_dict = {
        "id": [],
        "tags": [],
        "owner_contactid": [],
        "owner_email": [],
        "owner_name": [],
        "date_created": [],
        "agentid": [],
        "subject": []
    }

    for ticket in ticket_data:
        tickets_dict['id'].append(ticket.get("id"))
        tickets_dict['tags'].append(ticket.get("tags", []))
        tickets_dict['owner_contactid'].append(ticket.get("owner_contactid"))
        tickets_dict['owner_email'].append(ticket.get("owner_email"))
        tickets_dict['owner_name'].append(ticket.get("owner_name"))
        tickets_dict['date_created'].append(ticket.get("date_created"))
        tickets_dict['agentid'].append(ticket.get("agentid"))
        tickets_dict['subject'].append(ticket.get("subject"))

    return tickets_dict

def agents(max_pages: int = 5) -> dict:
    payload = {
        "_page": 1,
        "_perPage": 5
    }
    agents_data = paginate(
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

def get_ticket_messages(response: dict, agent_lookup: dict, max_pages: int = 5) -> pd.DataFrame:
    all_messages = []

    ticket_ids = response.get("id", [])
    owner_names = response.get("owner_name", [])
    subjects = response.get("subject", [])
    agentids = response.get("agentid", [])
    tags_list = response.get("tags", [])

    for i, ticket_id in enumerate(tqdm(ticket_ids, desc="Fetching ticket messages")):
        # Uncomment this one if gusto makita anong ticket ID ang nagpprocess
        # tqdm.write(f"Fetching ticket ID: {ticket_id}")
        url = f"{config.tickets_list_url}/{ticket_id}/messages"
        messages_data = paginate(
            url=url,
            payload=config.messages_payload.copy(),
            headers=config.headers,
            max_pages=max_pages
        )

        for item in messages_data:
            messages = item.get("messages", [])
            for message in messages:
                agent_id = agentids[i] if i < len(agentids) else None
                all_messages.append({
                    "ticket_id": ticket_id,
                    "owner_name": owner_names[i] if i < len(owner_names) else None,
                    "message_id": message.get("id"),
                    "subject": subjects[i] if i < len(subjects) else None,
                    "message": message.get("message"),
                    "dateCreated": message.get("datecreated"),
                    "type": message.get("type"),
                    "agentid": agent_id,
                    "agent_name": agent_lookup.get(agent_id),
                    "tags": ','.join(tags_list[i]) if i < len(tags_list) else None
                })

    return pd.DataFrame(all_messages)