import requests
from config import config

def ping() -> bool:
    response = requests.get(
        url=f"{config.base_url}/ping"
    )
    return response.status_code == 200, response

def tickets() -> dict:
    tickets_dict = {
        "id": [],
        "tags": [],
        "owner_contactid": [],
        "owner_email": [],
        "owner_name": [],
        "date_created": [],
        "agentid": []
    }
    response = requests.get(
        url=config.tickets_list_url,
        params=config.ticket_payload,
        headers=config.headers
    )
    tickets = response.json()
    for ticket in tickets[:config.ticket_payload["_perPage"]]:
        tickets_dict['id'].append(ticket.get("id"))
        tickets_dict['tags'].extend(ticket.get("tags", []))
        tickets_dict['owner_contactid'].append(ticket.get("owner_contactid"))
        tickets_dict['owner_email'].append(ticket.get("owner_email"))
        tickets_dict['owner_name'].append(ticket.get("owner_name"))
        tickets_dict['date_created'].append(ticket.get("date_created"))
        tickets_dict['agentid'].append(ticket.get("agentid"))
    return tickets_dict

def get_ticket_messages(response: dict):
    for k, ticket_ids in response.items():
        if k == "id":
            for ticket_id in ticket_ids:
                url = f"{config.tickets_list_url}/{ticket_id}/messages"
                print(f"Ticket URL: {url}")
                get_ticket = requests.get(
                    url=url,
                    params=config.messages_payload,
                    headers=config.headers
                )
                messages_data = get_ticket.json()
                for msg_ctr, item in enumerate(messages_data, start=1):
                    messages = item.get('messages', [])
                    print(item)
                    for message in messages:
                        print(f"Message #{msg_ctr}")
                        print(f"ID: {message['id']}")
                        print(f"Message: {message['message']}")
                        print(f"----\n")