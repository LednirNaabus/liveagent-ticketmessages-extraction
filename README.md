# LiveAgent Ticket Messages Extraction

This Python script interacts with the LiveAgent API to rerieve all messages associated with support tickets, filtered by a specific date range.

It's designed to streamline data extraction from the `/tickets/{ticket_id}/messages`. The extracted messages are then loaded into BigQuery for further analysis.

---

Before running the script, run:

```
pip install -r requirements.txt
```

# Usage

**Note:** Use `-h` for more info and help on the program.

## Max Pages
```
python main.py --max_pages [max_pages] --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD]
```
Where `max_pages` is the number of max pages you want to loop through. Alias is `-mp`.

## Per Page
```
python main.py --per_page [per_page] --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD]
```
Where `per_page` is the number of messages you want to extract. Alias is `-pp`.

## Skip BigQuery upload
```
python main.py -mp [max_pages] -pp [per_page] --skip_bq --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD]
```
Where `--skip_bq` lets you skip loading the data to BigQuery. Use this flag only when you want to see the data extracted and don't need to upload them to BigQuery. Alias is `-sbq`.

## Start and End date
```
python main.py -mp [max_pages] -pp [per_page] --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD]
```
Where `--start_date` and `--end_date` is the date range you want to extract from. Both expects an input in the following format: `YYYY-MM-DD`. Alias is `-sd` and `-ed` respectively. This flag is **required**.

## ID only
```
python main.py -mp [max_pages] -pp [per_page] --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD] --ids
```
Use `--ids` to fetch only IDs. Alias is `-i`.

## Weekly batches
```
python main.py -mp [max_pages] -pp [per_page] --start_date [YYYY-MM-DD] --end_date [YYYY-MM-DD] --weekly
```
Split the extraction into weeks. Use this flag when you expect the API to return a large number of tickets within the given date range. This helps avoid rate limiting issues by breaking down the data retrieval into smaller, manageable weekly chunks.

## Full example:
```
python main.py --max_pages 10 --per_page 100 --start_date 2025-01-01 --end_date 2025-01-31 --weekly
```
The example will extract data in weekly intervals from January 01, 2025 to January 31, 2025, using a maximum of 10 pages per request and 100 items per page. The absence of `--ids` indicate that all tickets and ticket messages will be extracted. Further, `--skip_bq` isn't used indicating that the retrieved data will be uploaded to BigQuery.

## Configuration
In the event you want to modify filters when making a request to the LiveAgent API, go to `config/config.py` and edit the `filters` variable.
```python
filters = json.dumps([
    ["filter1"],
    ["filter2"]
])
```
Refer to LiveAgent API for more information on their accepted API filters.

Do note that you will have to setup BigQuery credentials and API keys in order for the `bq_utils.py` to work.