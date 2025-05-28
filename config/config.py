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
agents_list_url = f"{base_url}/agents"
filters = json.dumps([
    ["date_created", "D>=", "2025-04-01 00:00:00"],
    ["date_created", "D<=", "2025-04-30 23:59:59"]
])
ticket_payload = {
    "_page": 1,
    "_perPage": 10,
    "_filters": filters
}
messages_payload = {
    "_page": 1,
    "_perPage": 10
}
headers = {
    'accept': 'application/json',
    'apikey': API_KEY
}

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(CONFIG_DIR, 'config.json')

# Google
with open(os.getenv('CREDENTIALS')) as f:
    creds = json.load(f)

with open(config_path, 'r') as file:
    json_config = json.load(file)

SCOPE = [
    'https://www.googleapis.com/auth/bigquery'
]

google_creds = service_account.Credentials.from_service_account_info(creds, scopes=SCOPE)
BQ_CLIENT = bigquery.Client(credentials=google_creds, project=google_creds.project_id)

GCLOUD_PROJECT_ID = json_config.get('BIGQUERY')['project_id']
BQ_DATASET_NAME = json_config.get('BIGQUERY')['dataset_name']
BQ_TABLE_NAME = json_config.get('BIGQUERY')['table_name']