import time
import app.store as store

def handle_data(c, cmd_p, target):
    """Menangani perintah data umum (SET, GET, KEYS, DEL, TYPE, INCR)"""
    from app.replication import propagate_command
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "SET":
        k, v = arg(1), arg(2)
        exp = None
        if len(cmd_p) > 4 and cmd_p[3].upper() == "PX":
            exp = time.time() + (int(cmd_p[4]) / 1000.0)
        store.DATA_STORE[k] = (v, exp)
        store.touch_key(k)
        target.sendall(b"+OK\r\n")
        propagate_command(cmd_p)
        return True

    elif c == "GET":
        k = arg(1)
        if k in store.DATA_STORE:
            val, ex = store.DATA_STORE[k]
            if ex and time.time() > ex:
                del store.DATA_STORE[k]
                store.touch_key(k)
                target.sendall(b"$-1\r\n")
            elif not isinstance(val, (str, bytes)):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        else: target.sendall(b"$-1\r\n")
        return True

    elif c == "KEYS":
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

    elif c == "INCR":
        k = arg(1)
        if k in store.DATA_STORE:
            v, ex = store.DATA_STORE[k]
            try:
                num = int(v) + 1
                store.DATA_STORE[k] = (str(num), ex)
                store.touch_key(k)
                target.sendall(f":{num}\r\n".encode())
            except: target.sendall(b"-ERR value is not an integer or out of range\r\n")
        else:
            store.DATA_STORE[k] = ("1", None)
            store.touch_key(k)
            target.sendall(b":1\r\n")
        propagate_command(cmd_p)
        return True

    elif c == "TYPE":
        k = arg(1)
        if k not in store.DATA_STORE: target.sendall(b"+none\r\n")
        else:
            v, ex = store.DATA_STORE[k]
            if ex and time.time() > ex:
                del store.DATA_STORE[k]
                store.touch_key(k); target.sendall(b"+none\r\n")
            else:
                if isinstance(v, (str, bytes)): target.sendall(b"+string\r\n")
                elif isinstance(v, list): target.sendall(b"+list\r\n")
                elif isinstance(v, store.Stream): target.sendall(b"+stream\r\n")
                else: target.sendall(b"+none\r\n")
        return True

    elif c == "DEL":
        keys = cmd_p[1:]
        count = 0
        with store.BLOCK_LOCK:
            for k in keys:
                if k in store.DATA_STORE:
                    del store.DATA_STORE[k]
                    store.touch_key(k); count += 1
        target.sendall(f":{count}\r\n".encode())
        propagate_command(cmd_p)
        return True
    
    return False
