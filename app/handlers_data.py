import time
import app.store as store

def handle_data(cmd_name, cmd_parts, target):
    """Handles generic data commands: SET, GET, KEYS, DEL, TYPE, INCR, EXISTS, EXPIRE, TTL."""
    from app.replication import propagate_command
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "SET":
        key, value = arg(1), arg(2)
        expiry = None
        if len(cmd_parts) > 4 and cmd_parts[3].upper() == "PX":
            expiry = time.time() + (int(cmd_parts[4]) / 1000.0)
        store.DATA_STORE[key] = (value, expiry)
        store.touch_key(key)
        target.sendall(b"+OK\r\n")
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "GET":
        key = arg(1)
        if key in store.DATA_STORE:
            val, ex = store.DATA_STORE[key]
            if ex and time.time() > ex:
                del store.DATA_STORE[key]
                store.touch_key(key)
                target.sendall(b"$-1\r\n")
            elif not isinstance(val, (str, bytes)):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        else: target.sendall(b"$-1\r\n")
        return True

    elif cmd_name == "KEYS":
        pattern = arg(1)
        if pattern == "*":
            all_keys = []
            now = time.time()
            with store.BLOCK_LOCK:
                for k, (v, exp) in list(store.DATA_STORE.items()):
                    if exp and now > exp: del store.DATA_STORE[k]
                    else: all_keys.append(k)
            res = f"*{len(all_keys)}\r\n"
            for k in all_keys: res += f"${len(k)}\r\n{k}\r\n"
            target.sendall(res.encode())
        else: target.sendall(b"*0\r\n")
        return True

    elif cmd_name == "INCR":
        key = arg(1)
        if key in store.DATA_STORE:
            v, ex = store.DATA_STORE[key]
            try:
                num = int(v) + 1
                store.DATA_STORE[key] = (str(num), ex)
                store.touch_key(key)
                target.sendall(f":{num}\r\n".encode())
            except: target.sendall(b"-ERR value is not an integer or out of range\r\n")
        else:
            store.DATA_STORE[key] = ("1", None)
            store.touch_key(key)
            target.sendall(b":1\r\n")
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "TYPE":
        key = arg(1)
        if key not in store.DATA_STORE: target.sendall(b"+none\r\n")
        else:
            v, ex = store.DATA_STORE[key]
            if ex and time.time() > ex:
                del store.DATA_STORE[key]
                store.touch_key(key); target.sendall(b"+none\r\n")
            else:
                if isinstance(v, (str, bytes)): target.sendall(b"+string\r\n")
                elif isinstance(v, list): target.sendall(b"+list\r\n")
                elif isinstance(v, store.Stream): target.sendall(b"+stream\r\n")
                elif isinstance(v, store.SortedSet): target.sendall(b"+zset\r\n")
                else: target.sendall(b"+none\r\n")
        return True

    elif cmd_name == "DEL":
        keys = cmd_parts[1:]
        count = 0
        with store.BLOCK_LOCK:
            for k in keys:
                if k in store.DATA_STORE:
                    del store.DATA_STORE[k]
                    store.touch_key(k); count += 1
        target.sendall(f":{count}\r\n".encode())
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "EXISTS":
        keys = cmd_parts[1:]
        count = 0
        now = time.time()
        for k in keys:
            if k in store.DATA_STORE:
                _, ex = store.DATA_STORE[k]
                if ex and now > ex:
                    del store.DATA_STORE[k]
                    store.touch_key(k)
                else: count += 1
        target.sendall(f":{count}\r\n".encode())
        return True

    elif cmd_name == "EXPIRE":
        key, sec = arg(1), int(arg(2))
        if key in store.DATA_STORE:
            v, _ = store.DATA_STORE[key]
            store.DATA_STORE[key] = (v, time.time() + sec)
            target.sendall(b":1\r\n")
            propagate_command(cmd_parts)
        else: target.sendall(b":0\r\n")
        return True

    elif cmd_name == "TTL":
        key = arg(1)
        if key in store.DATA_STORE:
            _, ex = store.DATA_STORE[key]
            if ex:
                remain = int(ex - time.time())
                if remain < 0:
                    del store.DATA_STORE[key]
                    store.touch_key(key); target.sendall(b":-2\r\n")
                else: target.sendall(f":{remain}\r\n".encode())
            else: target.sendall(b":-1\r\n")
        else: target.sendall(b":-2\r\n")
        return True
    
    return False
