import os
import logging
from core.extract_tags import extract_and_load_tags
from fastapi import FastAPI
from fastapi.responses import JSONResponse

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
        # print("Running extract_and_load_tags()")
        # r = await extract_and_load_tags()
        # print("Result: ", r)
        # print("Done running.")
        # if isinstance(r, dict):
        #     r = ensure_string_keys(r)
        # elif isinstance(r, list):
        #     r = [ensure_string_keys(item) if isinstance(item, dict) else item for item in r]
        # return r

        tags = await extract_and_load_tags()
        return JSONResponse(tags)
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))