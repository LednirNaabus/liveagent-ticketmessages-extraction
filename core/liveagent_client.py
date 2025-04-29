import os
import time
import requests
import pandas as pd
from config import config

MAX_PAGES = 10

def ping() -> bool:
    response = requests.get(
        url=f"{config.base_url}/ping"
    )
    return response.status_code == 200, response

def paginate(url: str, payload: dict, max_pages: int, headers: dict) -> list:
    all_data = []
    page = 1
    start_time = time.time()
    
    while page <= max_pages:
        payload["_page"] = page

        elapsed_time = time.time() - start_time
        print(f"Fetching page {page}: {elapsed_time:.1f}s elapsed")

        response = requests.get(url=url, params=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            print(f"No data found on page {page}.")
            break

        all_data.extend(data)
        page += 1

    total_elapsed_time = time.time() - start_time
    print(f"Finished fetching. Total time: {total_elapsed_time:1f}s.\n")
    return all_data

def tickets() -> dict:

    ticket_data = paginate(
        url=config.tickets_list_url,
        payload=config.ticket_payload.copy(),
        max_pages=MAX_PAGES,
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
        tickets_dict['tags'].extend(ticket.get("tags", []))
        tickets_dict['owner_contactid'].append(ticket.get("owner_contactid"))
        tickets_dict['owner_email'].append(ticket.get("owner_email"))
        tickets_dict['owner_name'].append(ticket.get("owner_name"))
        tickets_dict['date_created'].append(ticket.get("date_created"))
        tickets_dict['agentid'].append(ticket.get("agentid"))
        tickets_dict['subject'].append(ticket.get("subject"))

    return tickets_dict

def get_ticket_messages(response: dict) -> pd.DataFrame:
    all_messages = []

    for i, ticket_id in enumerate(response.get("id", [])):
        url = f"{config.tickets_list_url}/{ticket_id}/messages"
        messages_data = paginate(
            url=url,
            payload=config.messages_payload.copy(),
            headers=config.headers,
            max_pages=MAX_PAGES
        )

        for item in messages_data:
            messages = item.get("messages", [])
            for message in messages:
                all_messages.append({
                    "ticket_id": ticket_id,
                    "owner_name": response.get("owner_name", [None])[i],
                    "message_id": message.get("id"),
                    "subject": response.get("subject", [None])[i],
                    "message": message.get("message"),
                    "dateCreated": message.get("datecreated"),
                    "type": message.get("type"),
                    "agentid": response.get("agentid", [None])[i],
                    "tags": response.get("tags", [None])[i]
                })

    return pd.DataFrame(all_messages)