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

def ensure_string_keys(obj):
    if isinstance(obj, dict):
        return {str(k) if k is not None else "null": ensure_string_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [ensure_string_keys(item) for item in obj]
    else:
        return obj

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

        tags_dict = await extract_and_load_tags()
        for k in tags_dict.keys():
            if not isinstance(k, str):
                print(f"Invalid key: {k}")
        return JSONResponse(content=ensure_string_keys(tags_dict))
    except Exception as e:
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))