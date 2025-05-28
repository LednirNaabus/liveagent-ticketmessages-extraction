import logging
from core.extract_tags import extract_and_load_tags
from fastapi import FastAPI, HTTPException

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
        r = await extract_and_load_tags()
        return r
    except Exception as e:
        return {
            'error': str(e),
            'status': 'error'
        }