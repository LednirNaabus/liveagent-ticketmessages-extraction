# LiveAgent Ticket Messages Extraction

This Python script interacts with the LiveAgent API to rerieve all messages associated with support tickets, filtered by a specific date range.

It's designed to streamline data extraction from the `/tickets/{ticket_id}/messages`. The extracted messages are then loaded into BigQuery for further analysis.

## LiveAgent to BigQuery

1. Schema: `date_created`, `sender`, `receiver`, `message`, `ticket_id`, `tags`