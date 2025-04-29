from core.liveagent_client import tickets, get_ticket_messages, ping

if __name__ == "__main__":
    success, res = ping()
    if success:
        all_tickets = tickets()
        df = get_ticket_messages(all_tickets)
        print(df.head())
        print(df)
        df.to_csv("out.csv", index=False)
    else:
        print("Error API:")
        print(res)