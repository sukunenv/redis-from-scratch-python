import time
import app.store as store

def handle_list(c, cmd_p, target):
    """Menangani perintah List (LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE)"""
    from app.replication import propagate_command
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c in ["LPUSH", "RPUSH"]:
        k = arg(1)
        vals = cmd_p[2:]
        if k not in store.DATA_STORE: store.DATA_STORE[k] = ([], None)
        l, _ = store.DATA_STORE[k]
        if not isinstance(l, list):
            target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return True
        for v in vals:
            if c == "LPUSH": l.insert(0, v)
            else: l.append(v)
        store.touch_key(k)
        target.sendall(f":{len(l)}\r\n".encode())
        propagate_command(cmd_p)
        return True

    elif c in ["LPOP", "RPOP"]:
        k = arg(1)
        if k not in store.DATA_STORE: target.sendall(b"$-1\r\n")
        else:
            l, _ = store.DATA_STORE[k]
            if not isinstance(l, list):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            elif not l: target.sendall(b"$-1\r\n")
            else:
                v = l.pop(0) if c == "LPOP" else l.pop()
                store.touch_key(k)
                target.sendall(f"${len(v)}\r\n{v}\r\n".encode())
                propagate_command(cmd_p)
        return True

    elif c == "LLEN":
        k = arg(1)
        if k not in store.DATA_STORE: target.sendall(b":0\r\n")
        else:
            l, _ = store.DATA_STORE[k]
            target.sendall(f":{len(l) if isinstance(l, list) else 0}\r\n".encode())
        return True

    elif c == "LRANGE":
        k, start, stop = arg(1), int(arg(2)), int(arg(3))
        if k not in store.DATA_STORE: target.sendall(b"*0\r\n")
        else:
            l, _ = store.DATA_STORE[k]
            if not isinstance(l, list): target.sendall(b"-WRONGTYPE ...\r\n")
            else:
                if stop < 0: stop = len(l) + stop
                res_l = l[start : stop + 1]
                res = f"*{len(res_l)}\r\n"
                for i in res_l: res += f"${len(i)}\r\n{i}\r\n"
                target.sendall(res.encode())
        return True

    elif c == "LINDEX":
        k, idx = arg(1), int(arg(2))
        if k not in store.DATA_STORE: target.sendall(b"$-1\r\n")
        else:
            l, _ = store.DATA_STORE[k]
            if not isinstance(l, list): target.sendall(b"-WRONGTYPE ...\r\n")
            elif idx < 0 or idx >= len(l): target.sendall(b"$-1\r\n")
            else:
                v = l[idx]
                target.sendall(f"${len(v)}\r\n{v}\r\n".encode())
        return True

    elif c in ["BLPOP", "BRPOP"]:
        # Format: BLPOP key [key ...] timeout
        # Versi sederhana: hanya dukung 1 key untuk tahap ini
        k, timeout_sec = arg(1), float(arg(2))
        start_wait = time.time()
        
        while True:
            if k in store.DATA_STORE:
                l, _ = store.DATA_STORE[k]
                if isinstance(l, list) and l:
                    v = l.pop(0) if c == "BLPOP" else l.pop()
                    store.touch_key(k)
                    # Balas dengan Array [key, value]
                    res = f"*2\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n"
                    target.sendall(res.encode())
                    return True
            
            # Cek timeout
            if (time.time() - start_wait) >= timeout_sec:
                target.sendall(b"*-1\r\n") # Null array jika timeout
                return True
            
            time.sleep(0.05) # Istirahat sejenak
    
    return False
