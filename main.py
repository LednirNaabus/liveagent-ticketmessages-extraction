import argparse
from core.liveagent_client import tickets, get_ticket_messages, ping

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch tickets and messages using LiveAgent API.")
    parser.add_argument(
        "--mp",
        type=int,
        default=5,
        help="Max pages to fetch (default: 5, max page is '_perPage' in LiveAgent API)"
    )
    args = parser.parse_args()
    success, res = ping()
    if success:
        all_tickets = tickets(max_pages=args.mp)
        df = get_ticket_messages(all_tickets, max_pages=args.mp)
        print(df)
        df.to_csv("out.csv", index=False)
    else:
        print("Error API:")
        print(res)