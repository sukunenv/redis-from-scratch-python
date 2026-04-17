import app.store as store

def handle_pubsub(c, cmd_p, target, session):
    """Menangani perintah SUBSCRIBE, UNSUBSCRIBE, dan PUBLISH"""
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "SUBSCRIBE":
        channel = arg(1) or ""
        count = 1
        if session is not None:
            session["subscribed_channels"].add(channel)
            count = len(session["subscribed_channels"])
            with store.BLOCK_LOCK:
                if channel not in store.SUBSCRIBERS:
                    store.SUBSCRIBERS[channel] = set()
                store.SUBSCRIBERS[channel].add(target)
        res = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n"
        target.sendall(res.encode())
        return True

    elif c == "UNSUBSCRIBE":
        channel = arg(1) or ""
        count = 0
        if session is not None:
            session["subscribed_channels"].discard(channel)
            count = len(session["subscribed_channels"])
            with store.BLOCK_LOCK:
                if channel in store.SUBSCRIBERS:
                    store.SUBSCRIBERS[channel].discard(target)
        res = f"*3\r\n$11\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n"
        target.sendall(res.encode())
        return True

    elif c == "PUBLISH":
        channel = arg(1) or ""
        message = arg(2) or ""
        with store.BLOCK_LOCK:
            subs = store.SUBSCRIBERS.get(channel, set())
            count = len(subs)
            msg_resp = f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n".encode()
            for sub_conn in subs:
                try: sub_conn.sendall(msg_resp)
                except: pass
        target.sendall(f":{count}\r\n".encode())
        return True
    
    return False
