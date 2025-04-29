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
python main.py --max_pages [max_pages]
```
Where `max_pages` is the number of max pages you want to loop through. 

## Per Page
```
python main.py --per_page [per_page]
```
Where `per_page` is the number of messages you want to extract.

## Full example:
```
python main.py --max_pages 10 --per_page 100
```
The example command above will gather 100 messages per ticket and do this until it reaches a maximum page of 10.

## Configuration

To change the date range, go to `config/config.py` and change the date filter:
```python
filters = json.dumps([[
    "date_created", "D>", "2025-01-01 00:00:00"
]])
```
Refer to LiveAgent API for more information on their accepted API filters.

**Note**: Add `date` as command argument in the future.

## LiveAgent to BigQuery

1. Schema: `date_created`, `sender`, `receiver`, `message`, `ticket_id`, `tags`