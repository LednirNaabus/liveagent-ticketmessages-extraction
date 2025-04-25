import os
import json
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

base_url = "https://mechanigo.ladesk.com/api/v3"
tickets_list_url = f"{base_url}/tickets"
filters = json.dumps([[
    "date_created", "D>", "2025-01-01 00:00:00"
]])
ticket_payload = {
    "_page": 1,
    "_perPage": 3,
    "_filters": filters
}
messages_payload = {
    "_page": 1,
    "_perPage": 3
}
headers = {
    'accept': 'application/json',
    'apikey': API_KEY
}