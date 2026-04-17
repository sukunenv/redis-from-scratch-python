import time
import app.store as store

def handle_list(cmd_name, cmd_parts, target, session=None):
    """Handles Redis List commands: LPUSH, RPUSH, LLEN, LRANGE, LINDEX, BLPOP, BRPOP."""
    from app.replication import propagate_command
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name in ["LPUSH", "RPUSH"]:
        key = arg(1)
        values = cmd_parts[2:]
        if key not in store.DATA_STORE:
            store.DATA_STORE[key] = ([], None)
        
        lst, _ = store.DATA_STORE[key]
        if not isinstance(lst, list):
            target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return True
            
        for v in values:
            if cmd_name == "LPUSH": lst.insert(0, v)
            else: lst.append(v)
            
        store.touch_key(key)
        target.sendall(f":{len(lst)}\r\n".encode())
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "LLEN":
        key = arg(1)
        if key not in store.DATA_STORE:
            target.sendall(b":0\r\n")
        else:
            lst, _ = store.DATA_STORE[key]
            target.sendall(f":{len(lst) if isinstance(lst, list) else 0}\r\n".encode())
        return True

    elif cmd_name == "LRANGE":
        key, start, stop = arg(1), int(arg(2)), int(arg(3))
        if key not in store.DATA_STORE:
            target.sendall(b"*0\r\n")
        else:
            lst, _ = store.DATA_STORE[key]
            if not isinstance(lst, list):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                if stop < 0: stop = len(lst) + stop
                res_l = lst[start : stop + 1]
                res = f"*{len(res_l)}\r\n"
                for item in res_l:
                    res += f"${len(item)}\r\n{item}\r\n"
                target.sendall(res.encode())
        return True

    elif cmd_name == "LINDEX":
        key, idx = arg(1), int(arg(2))
        if key not in store.DATA_STORE:
            target.sendall(b"$-1\r\n")
        else:
            lst, _ = store.DATA_STORE[key]
            if not isinstance(lst, list):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            elif idx < 0 or idx >= len(lst):
                target.sendall(b"$-1\r\n")
            else:
                val = lst[idx]
                target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        return True

    elif cmd_name in ["BLPOP", "BRPOP"]:
        # Basic implementation: Supports single key for this stage
        key, timeout_sec = arg(1), float(arg(2))
        
        while True:
            if key in store.DATA_STORE:
                lst, _ = store.DATA_STORE[key]
                if isinstance(lst, list) and lst:
                    val = lst.pop(0) if cmd_name == "BLPOP" else lst.pop()
                    store.touch_key(key)
                    # Response: Array [key, value]
                    res = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                    target.sendall(res.encode())
                    return True
            
            # Simple polling for blocking commands
            time.sleep(0.1)
        return True
    
    return False
