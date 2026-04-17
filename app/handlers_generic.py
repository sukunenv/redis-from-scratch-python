import app.store as store

def handle_generic(cmd_name, cmd_parts, target, session=None):
    """Handles basic Redis commands: PING, ECHO, CONFIG, COMMAND."""
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "PING":
        target.sendall(b"+PONG\r\n")
        return True

    elif cmd_name == "ECHO":
        msg = arg(1)
        target.sendall(f"${len(msg)}\r\n{msg}\r\n".encode())
        return True

    elif cmd_name == "CONFIG":
        sub = arg(1).upper()
        if sub == "GET":
            key = arg(2).lower()
            val = store.CONFIG.get(key, "")
            res = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
            target.sendall(res.encode())
        return True

    elif cmd_name == "BGREWRITEAOF":
        import threading
        from app.aof import rewrite_aof
        
        def run_rewrite():
            rewrite_aof()
            
        threading.Thread(target=run_rewrite, daemon=True).start()
        target.sendall(b"+Background rewriting of AOF started\r\n")
        return True

    elif cmd_name == "COMMAND":

        # Basic response for redis-cli metadata discovery
        target.sendall(b"*0\r\n")
        return True

    return False
