import os
import logging
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from core.extract_tags import extract_and_load_tags
from core.extract_tickets_date import extract_tickets, extract_ticket_messages

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

app = FastAPI()

@app.get("/")
def root():
    """
    Home - for testing purposes.
    """
    return {"message": "Hello World"}

@app.post("/mechanigo-liveagent/update-tags/{table_name}")
async def update_tags(table_name: str):
    """
    To update & run tags daily.
    It starts from fetching the tags data from the LiveAgent API through the `/tags` endpoint.
    Finally, it is loaded to BigQuery.
    """
    try:
        tags = await extract_and_load_tags(table_name)
        return JSONResponse(tags)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-tickets/{table_name}")
async def update_tickets(table_name: str):
    """
    To update & run tickets daily.
    It starts from fetching the tickets data from the LiveAgent API through the `/tickets` endpoint.
    It is then loaded to BigQuery.
    """
    try:
        now = pd.Timestamp.now().tz_localize('Asia/Manila')
        print(f"NOW: {now}")
        date = now - pd.Timedelta(hours=6)
        logger.info(f"Date and time Ran: {date}")
        tickets = await extract_tickets(date, table_name)
        return JSONResponse(tickets)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-ticket-messages/{table_name}")
async def update_ticket_messages(table_name: str):
    """
    To update & run ticket messages daily.
    It starts from fetching the ticket messages from the LiveAgent API through the `/tickets/{ticket_id}/messages` endpoint.
    It is then loaded to BigQuery.
    """
    try:
        ticket_messages = await extract_ticket_messages(table_name)
        return JSONResponse(ticket_messages)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))