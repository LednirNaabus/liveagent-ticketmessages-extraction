import os
import json
from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
API_KEY = os.getenv("API_KEY")

# API stuff
base_url = "https://mechanigo.ladesk.com/api/v3"
tickets_list_url = f"{base_url}/tickets"
filters = json.dumps([[
    "date_created", "D>", "2025-01-01 00:00:00"
]])
ticket_payload = {
    "_page": 1,
    "_perPage": 1000,
    "_filters": filters
}
messages_payload = {
    "_page": 1,
    "_perPage": 1000
}
headers = {
    'accept': 'application/json',
    'apikey': API_KEY
}

# Google stuff
GOOGLE_API_CREDS_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_API_CREDS = os.path.join(GOOGLE_API_CREDS_DIR, 'google-api-key.json')

with open(GOOGLE_API_CREDS, 'r') as file:
    creds = json.load(file)

SCOPE = [
    'https://www.googleapis.com/auth/bigquery'
]

google_creds = service_account.Credentials.from_service_account_info(creds, scopes=SCOPE)
BQ_CLIENT = bigquery.Client(credentials=google_creds, project=google_creds.project_id)

bq_config = {
    "project_id": "absolute-gantry-363408",
    "dataset_name": "mechanigo_liveagent",
    "table_name": "liveagent_messages"
}

GCLOUD_PROJECT_ID = bq_config["project_id"]
BQ_DATASET_NAME = bq_config["dataset_name"]
BQ_TABLE_NAME = bq_config["table_name"]