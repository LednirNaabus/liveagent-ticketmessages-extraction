import os
import time
import requests
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
        "agentid": []
    }

    page = 1
    per_page = config.ticket_payload["_perPage"]

    while True:
        payload = config.ticket_payload.copy()
        payload["_page"] = page

        response = requests.get(
            url=config.tickets_list_url,
            params=config.ticket_payload,
            headers=config.headers
        )

        response.raise_for_status()
        tickets = response.json()

        if isinstance(tickets, dict):
            tickets = tickets.get("data", [])

        print(f"Fetched {len(tickets)} tickets from page {page}.")
        elapsed = time.time() - start_time
        print(f"Time elapsed: {elapsed:.2f} seconds")

        for ticket in tickets:
            tickets_dict['id'].append(ticket.get("id"))
            tickets_dict['tags'].extend(ticket.get("tags", []))
            tickets_dict['owner_contactid'].append(ticket.get("owner_contactid"))
            tickets_dict['owner_email'].append(ticket.get("owner_email"))
            tickets_dict['owner_name'].append(ticket.get("owner_name"))
            tickets_dict['date_created'].append(ticket.get("date_created"))
            tickets_dict['agentid'].append(ticket.get("agentid"))

        if len(tickets) < per_page:
            break

        page += 1

    total_elapsed_time = time.time() - start_time
    print(f"Finished fetching all tickets. Total: time: {total_elapsed_time:.2f} seconds.")

    return tickets_dict

def get_ticket_messages(response: dict):

    start_time = time.time()
    for k, ticket_ids in response.items():
        if k == "id":
            for ticket_id in ticket_ids:
                page = 1
                per_page = config.messages_payload["_perPage"]

                while True:
                    payload = config.messages_payload.copy()
                    payload["_page"] = page
                    url = f"{config.tickets_list_url}/{ticket_id}/messages"
                    print(f"Fetching messages from Ticket ID {ticket_id} (Page {page})")

                    get_ticket = requests.get(
                        url=url,
                        params=config.messages_payload,
                        headers=config.headers
                    )
                    get_ticket.raise_for_status()
                    messages_data = get_ticket.json()

                    if isinstance(messages_data, dict):
                        messages_data = messages_data.get("data", [])

                    if not messages_data:
                        print(f"No messages found for ticket ID {ticket_id}")
                        break

                    for msg_ctr, item in enumerate(messages_data, start=1):
                        messages = item.get('messages', [])
                        print(item)
                        for message in messages:
                            print(f"Message #{msg_ctr}")
                            print(f"ID: {message['id']}")
                            print(f"Message: {message['message']}")
                            print(f"----\n")

                    elapsed = time.time() - start_time
                    print(f"Time elapsed: {elapsed:.2f} seconds\n")
                    
                    if len(messages_data) < per_page:
                        break

                    page += 1

    total_elapsed_time = time.time() - start_time
    print(f"Finished fetching all messages. Total time: {total_elapsed_time:.2f} seconds.")