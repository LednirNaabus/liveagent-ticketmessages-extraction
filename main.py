from core.liveagent_client import tickets, get_ticket_messages, ping

if __name__ == "__main__":
    success, res = ping()
    if success:
        all_tickets = tickets()
        get_ticket_messages(all_tickets)
    else:
        print("Error API:")
        print(res)