# LiveAgent Ticket Messages Extraction

This Python script interacts with the LiveAgent API to rerieve all messages associated with support tickets, filtered by a specific date range.

It's designed to streamline data extraction from the `/tickets/{ticket_id}/messages`. The extracted messages are then loaded into BigQuery for further analysis.

---

Before running the script, run:

```
pip install -r requirements.txt
```

# Usage

```
python main.py --mp [max_pages]
```
Where `max_pages` is the number of max pages you want to loop through. Use `-h` for more info on the program.

## LiveAgent to BigQuery

1. Schema: `date_created`, `sender`, `receiver`, `message`, `ticket_id`, `tags`