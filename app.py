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

@app.post("/mechanigo-liveagent/update-tags")
async def update_tags():
    """
    To update & run tags daily.
    It starts from fetching the tags data from the LiveAgent API through the `/tags` endpoint.
    Finally, it is loaded to BigQuery.
    """
    try:
        tags = await extract_and_load_tags()
        return JSONResponse(tags)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-tickets")
async def update_tickets():
    """
    To update & run tickets daily.
    It starts from fetching the tickets data from the LiveAgent API through the `/tickets` endpoint.
    It is then loaded to BigQuery.
    """
    try:
        date = pd.Timestamp.now().tz_localize('Asia/Manila')
        tickets = await extract_tickets(date)
        return JSONResponse(tickets)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-ticket-messages")
async def update_ticket_messages():
    """
    To update & run ticket messages daily.
    It starts from fetching the ticket messages from the LiveAgent API through the `/tickets/{ticket_id}/messages` endpoint.
    It is then loaded to BigQuery.
    """
    try:
        ticket_messages = await extract_ticket_messages()
        return JSONResponse(ticket_messages)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))