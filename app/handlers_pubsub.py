import app.store as store

def handle_pubsub(cmd_name, cmd_parts, target, session):
    """Handles Redis Pub/Sub commands: SUBSCRIBE, UNSUBSCRIBE, PUBLISH."""
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "SUBSCRIBE":
        channels = cmd_parts[1:]
        with store.BLOCK_LOCK:
            for channel in channels:
                if channel not in store.SUBSCRIBERS:
                    store.SUBSCRIBERS[channel] = set()
                store.SUBSCRIBERS[channel].add(target)
                session["subscribed_channels"].add(channel)
                
                # Response format: [ "subscribe", channel, count ]
                count = len(session["subscribed_channels"])
                res = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n"
                target.sendall(res.encode())
        return True

    elif cmd_name == "PUBLISH":
        channel, message = arg(1), arg(2)
        count = 0
        with store.BLOCK_LOCK:
            if channel in store.SUBSCRIBERS:
                # Format message for subscribers: [ "message", channel, content ]
                res = f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n"
                msg_bytes = res.encode()
                
                # Create a list to iterate over to avoid issues if set changes
                for sub in list(store.SUBSCRIBERS[channel]):
                    try:
                        sub.sendall(msg_bytes)
                        count += 1
                    except:
                        store.SUBSCRIBERS[channel].discard(sub)
                        
        target.sendall(f":{count}\r\n".encode())
        return True

    return False
