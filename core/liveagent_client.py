import os
import time
import requests
import pandas as pd
from config import config

def ping() -> bool:
    response = requests.get(
        url=f"{config.base_url}/ping"
    )
    return response.status_code == 200, response

def tickets() -> dict:

    start_time = time.time()
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

    MAX_PAGES = 2
    page = 1

    while page <= MAX_PAGES:
        payload = config.ticket_payload.copy()
        payload["_page"] = page

        response = requests.get(
            url=config.tickets_list_url,
            params=payload,
            headers=config.headers
        )

        response.raise_for_status()
        tickets = response.json()

        if isinstance(tickets, dict):
            tickets = tickets.get("data", [])

        if not tickets:
            print(f"No tickets found on page {page}.")
            break

        for ticket in tickets:
            tickets_dict['id'].append(ticket.get("id"))
            tickets_dict['tags'].extend(ticket.get("tags", []))
            tickets_dict['owner_contactid'].append(ticket.get("owner_contactid"))
            tickets_dict['owner_email'].append(ticket.get("owner_email"))
            tickets_dict['owner_name'].append(ticket.get("owner_name"))
            tickets_dict['date_created'].append(ticket.get("date_created"))
            tickets_dict['agentid'].append(ticket.get("agentid"))
            tickets_dict['subject'].append(ticket.get("subject"))

        elapsed = time.time() - start_time
        print(f"Time elapsed: {elapsed:.2f} seconds")

        page += 1

    total_elapsed_time = time.time() - start_time
    print(f"Finished fetching all tickets. Total: time: {total_elapsed_time:.2f} seconds.")

    return tickets_dict

def get_ticket_messages(response: dict) -> pd.DataFrame:
    MAX_PAGES = 5
    all_messages = []
    start_time = time.time()
    for k, ticket_ids in response.items():
        if k == "id":
            for ticket_id in ticket_ids:
                page = 1

                try:
                    idx = response["id"].index(ticket_id)

                except ValueError:
                    print(f"Ticket ID {ticket_id} not found in response['id']")
                    continue

                while page <= MAX_PAGES:
                    payload = config.messages_payload.copy()
                    payload["_page"] = page
                    url = f"{config.tickets_list_url}/{ticket_id}/messages"
                    print(f"Fetching messages from Ticket ID {ticket_id} (Page {page})")

                    get_ticket = requests.get(
                        url=url,
                        params=payload,
                        headers=config.headers
                    )
                    get_ticket.raise_for_status()
                    messages_data = get_ticket.json()

                    if isinstance(messages_data, dict):
                        messages_data = messages_data.get("data", [])

                    if not messages_data:
                        print(f"No messages found for ticket ID {ticket_id}")
                        break

                    for item in messages_data:
                        messages = item.get('messages', [])
                        for message in messages:
                            all_messages.append({
                                "ticket_id": ticket_id,
                                "owner_name": response.get("owner_name", [None])[idx],
                                "message_id": message.get("id"),
                                "subject": response.get("subject", [None])[idx],
                                "message": message.get("message"),
                                "dateCreated": message.get("datecreated"),
                                "type": message.get("type"),
                                "agentid": response.get("agentid", [None])[idx],
                                "tags": response.get("tags", [None])[idx]
                            })

                    elapsed = time.time() - start_time
                    print(f"Time elapsed: {elapsed:.2f} seconds\n")
                    
                    page += 1

    total_elapsed_time = time.time() - start_time
    print(f"Finished fetching all messages. Total time: {total_elapsed_time:.2f} seconds.")
    messages_df = pd.DataFrame(all_messages)
    return messages_df