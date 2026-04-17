import socket
import threading
import time

# Gudang Data Utama
DATA_STORE = {}

class Stream:
    def __init__(self):
        self.entries = []

# Peralatan Sinkronisasi
BLOCK_LOCK = threading.Lock()
BLOCKING_CLIENTS = {}
STREAM_BLOCKING_CLIENTS = {}

def format_xread_data(data):
    if not data: return "*-1\r\n"
    o = f"*{len(data)}\r\n"
    for k, ents in data:
        o += f"*2\r\n${len(k)}\r\n{k}\r\n*{len(ents)}\r\n"
        for eid, flds in ents:
            o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
            for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
    return o

def parse_resp(data):
    if not data: return []
    try:
        lines = data.decode().split("\r\n")
    except UnicodeDecodeError: return []
    
    commands = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line:
            i += 1
            continue
        if line.startswith('*'):
            try:
                num_args = int(line[1:])
                cmd_parts = []
                i += 1
                for _ in range(num_args):
                    if i + 1 < len(lines):
                        cmd_parts.append(lines[i+1])
                        i += 2
                if len(cmd_parts) == num_args:
                    commands.append(cmd_parts)
            except (ValueError, IndexError):
                i += 1
        else: i += 1
    return commands

def handle_client(connection):
    try:
        is_transaction_active = False
        transaction_queue = []

        while True:
            raw_data = connection.recv(4096)
            if not raw_data: break

            all_cmds = parse_resp(raw_data)
            for p in all_cmds:
                if not p: continue
                
                cmd_name = p[0].upper()

                if is_transaction_active and cmd_name not in ["EXEC", "DISCARD"]:
                    transaction_queue.append(p)
                    connection.sendall(b"+QUEUED\r\n")
                    continue

                to_run = []
                is_exec = False

                if cmd_name == "MULTI":
                    is_transaction_active = True
                    connection.sendall(b"+OK\r\n")
                    continue
                elif cmd_name == "DISCARD":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        is_transaction_active = False
                        transaction_queue = []
                        connection.sendall(b"+OK\r\n")
                    continue
                elif cmd_name == "EXEC":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    is_exec = True
                    is_transaction_active = False
                    to_run = list(transaction_queue)
                    transaction_queue = []
                else:
                    to_run = [p]

                res_list = []
                for cmd_p in to_run:
                    class Proxy:
                        def __init__(self): self.buf = b""
                        def sendall(self, d): self.buf += d
                    
                    target = connection
                    if is_exec:
                        proxy = Proxy()
                        target = proxy

                    try:
                        c = cmd_p[0].upper()
                        def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

                        if c == "PING":
                            target.sendall(b"+PONG\r\n")

                        elif c == "ECHO":
                            val = arg(1) or ""
                            target.sendall(f"${len(val)}\r\n{val}\r\n".encode())

                        elif c == "SET":
                            k, v = arg(1), arg(2)
                            exp = None
                            if len(cmd_p) > 4 and cmd_p[3].upper() == "PX":
                                exp = time.time() + (int(cmd_p[4]) / 1000.0)
                            DATA_STORE[k] = (v, exp)
                            target.sendall(b"+OK\r\n")

                        elif c == "GET":
                            k = arg(1)
                            if k in DATA_STORE:
                                val, ex = DATA_STORE[k]
                                if ex and time.time() > ex:
                                    del DATA_STORE[k]
                                    target.sendall(b"$-1\r\n")
                                elif not isinstance(val, str):
                                    target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                else:
                                    target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
                            else: target.sendall(b"$-1\r\n")

                        elif c == "INCR":
                            k = arg(1)
                            if k in DATA_STORE:
                                v, ex = DATA_STORE[k]
                                try:
                                    num = int(v) + 1
                                    DATA_STORE[k] = (str(num), ex)
                                    target.sendall(f":{num}\r\n".encode())
                                except (ValueError, TypeError):
                                    target.sendall(b"-ERR value is not an integer or out of range\r\n")
                            else:
                                DATA_STORE[k] = ("1", None)
                                target.sendall(b":1\r\n")

                        elif c == "TYPE":
                            k = arg(1)
                            if k not in DATA_STORE: target.sendall(b"+none\r\n")
                            else:
                                v, ex = DATA_STORE[k]
                                if ex and time.time() > ex:
                                    del DATA_STORE[k]
                                    target.sendall(b"+none\r\n")
                                else:
                                    if isinstance(v, str): target.sendall(b"+string\r\n")
                                    elif isinstance(v, list): target.sendall(b"+list\r\n")
                                    elif isinstance(v, Stream): target.sendall(b"+stream\r\n")
                                    else: target.sendall(b"+none\r\n")

                        elif c == "XADD":
                            k, eid = arg(1), arg(2)
                            if eid == "*":
                                ms = int(time.time() * 1000)
                                if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream) and DATA_STORE[k][0].entries:
                                    l_id = DATA_STORE[k][0].entries[-1][0]
                                    l_ms, l_seq = map(int, l_id.split("-"))
                                    seq = l_seq + 1 if ms == l_ms else 0
                                    ms = max(ms, l_ms)
                                    eid = f"{ms}-{seq}"
                                else: eid = f"{ms}-0"
                            elif eid.endswith("-*"):
                                ms = int(eid.split("-")[0])
                                if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream) and DATA_STORE[k][0].entries:
                                    l_id = DATA_STORE[k][0].entries[-1][0]
                                    l_ms, l_seq = map(int, l_id.split("-"))
                                    seq = l_seq + 1 if ms == l_ms else (0 if ms > 0 else 1)
                                    eid = f"{ms}-{seq}"
                                else: eid = f"{ms}-{0 if ms > 0 else 1}"

                            if eid == "0-0": target.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                            else:
                                flds = {}
                                for idx in range(3, len(cmd_p)-1, 2): flds[cmd_p[idx]] = cmd_p[idx+1]
                                valid = True
                                if k in DATA_STORE:
                                    s, _ = DATA_STORE[k]
                                    if isinstance(s, Stream) and s.entries:
                                        lm, ls = map(int, s.entries[-1][0].split("-"))
                                        nm, ns = map(int, eid.split("-"))
                                        if nm < lm or (nm == lm and ns <= ls): valid = False
                                    if valid and isinstance(s, Stream): s.entries.append((eid, flds))
                                else:
                                    s = Stream()
                                    s.entries.append((eid, flds))
                                    DATA_STORE[k] = (s, None)
                                if valid:
                                    target.sendall(f"${len(eid)}\r\n{eid}\r\n".encode())
                                    with BLOCK_LOCK:
                                        if k in STREAM_BLOCKING_CLIENTS:
                                            for bel, _ in STREAM_BLOCKING_CLIENTS[k]: bel.set()
                                else: target.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

                        elif c == "XRANGE":
                            k, start, end = arg(1), arg(2), arg(3)
                            def p_id(rid, dflt):
                                if rid == "-": return 0, 0
                                if rid == "+": return float('inf'), float('inf')
                                ps = rid.split("-")
                                return int(ps[0]), (int(ps[1]) if len(ps)>1 else dflt)
                            sm, ss = p_id(start, 0)
                            em, es = p_id(end, float('inf'))
                            res = []
                            if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream):
                                for eid, flds in DATA_STORE[k][0].entries:
                                    m, s = map(int, eid.split("-"))
                                    if (m > sm or (m == sm and s >= ss)) and (m < em or (m == em and s <= es)):
                                        res.append((eid, flds))
                            o = f"*{len(res)}\r\n"
                            for eid, flds in res:
                                o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
                                for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
                            target.sendall(o.encode())

                        elif c == "XREAD":
                            u_p = [x.upper() for x in cmd_p]
                            b_ms = int(cmd_p[u_p.index("BLOCK")+1]) if "BLOCK" in u_p else -1
                            s_idx = u_p.index("STREAMS")
                            args = cmd_p[s_idx+1:]
                            n = len(args)//2
                            ks, rids = args[:n], args[n:]
                            ids = []
                            for kk, rid in zip(ks, rids):
                                if rid == "$" and kk in DATA_STORE and isinstance(DATA_STORE[kk][0], Stream) and DATA_STORE[kk][0].entries:
                                    ids.append(DATA_STORE[kk][0].entries[-1][0])
                                else: ids.append("0-0" if rid == "$" else rid)

                            def fetch():
                                fnd = []
                                for kk, bid in zip(ks, ids):
                                    bm, bs = map(int, bid.split("-"))
                                    if kk in DATA_STORE and isinstance(DATA_STORE[kk][0], Stream):
                                        mtch = [(eid, f) for eid, f in DATA_STORE[kk][0].entries if int(eid.split("-")[0]) > bm or (int(eid.split("-")[0]) == bm and int(eid.split("-")[1]) > bs)]
                                        if mtch: fnd.append((kk, mtch))
                                return fnd

                            init = fetch()
                            if init: target.sendall(format_xread_data(init).encode())
                            elif b_ms >= 0:
                                bel = threading.Event()
                                with BLOCK_LOCK:
                                    for kk, bid in zip(ks, ids):
                                        if kk not in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[kk] = []
                                        STREAM_BLOCKING_CLIENTS[kk].append((bel, bid))
                                woke = bel.wait(b_ms/1000.0) if b_ms > 0 else (bel.wait() or True)
                                with BLOCK_LOCK:
                                    for kk in ks:
                                        if kk in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[kk] = [x for x in STREAM_BLOCKING_CLIENTS[kk] if x[0] != bel]
                                fin = fetch() if woke else None
                                target.sendall(format_xread_data(fin).encode() if fin else b"*-1\r\n")
                            else: target.sendall(b"*-1\r\n")

                        elif c in ["RPUSH", "LPUSH"]:
                            k, items = arg(1), cmd_p[2:]
                            if c == "LPUSH": items = list(reversed(items))
                            if k in DATA_STORE and isinstance(DATA_STORE[k][0], list):
                                l, ex = DATA_STORE[k]
                                if c == "RPUSH": l.extend(items)
                                else: l = items + l
                                DATA_STORE[k] = (l, ex)
                            else: DATA_STORE[k] = (items, None)
                            target.sendall(f":{len(DATA_STORE[k][0])}\r\n".encode())
                            with BLOCK_LOCK:
                                while k in BLOCKING_CLIENTS and BLOCKING_CLIENTS[k] and DATA_STORE[k][0]:
                                    it = DATA_STORE[k][0].pop(0)
                                    bl, box = BLOCKING_CLIENTS[k].pop(0)
                                    box.append(it)
                                    bl.set()

                        elif c == "LRANGE":
                            k, st, sp = arg(1), int(arg(2)), int(arg(3))
                            if k in DATA_STORE and isinstance(DATA_STORE[k][0], list):
                                l = DATA_STORE[k][0]
                                if st < 0: st += len(l)
                                if sp < 0: sp += len(l)
                                sl = l[max(0, st):sp+1]
                                o = f"*{len(sl)}\r\n"
                                for x in sl: o += f"${len(x)}\r\n{x}\r\n"
                                target.sendall(o.encode())
                            else: target.sendall(b"*0\r\n")

                        elif c == "LPOP":
                            k, cn = arg(1), (int(arg(2)) if len(cmd_p)>2 else None)
                            if k in DATA_STORE and isinstance(DATA_STORE[k][0], list) and DATA_STORE[k][0]:
                                l, ex = DATA_STORE[k]
                                if cn:
                                    tk, rm = l[:cn], l[cn:]
                                    DATA_STORE[k] = (rm, ex)
                                    o = f"*{len(tk)}\r\n"
                                    for x in tk: o += f"${len(x)}\r\n{x}\r\n"
                                    target.sendall(o.encode())
                                else:
                                    it = l.pop(0)
                                    target.sendall(f"${len(it)}\r\n{it}\r\n".encode())
                            else: target.sendall(b"$-1\r\n")

                        elif c == "BLPOP":
                            k, t_out = arg(1), float(arg(2))
                            wait_bel, wait_box = None, []
                            with BLOCK_LOCK:
                                if k in DATA_STORE and isinstance(DATA_STORE[k][0], list) and DATA_STORE[k][0]:
                                    it = DATA_STORE[k][0].pop(0)
                                    target.sendall(f"*2\r\n${len(k)}\r\n{k}\r\n${len(it)}\r\n{it}\r\n".encode())
                                else:
                                    wait_bel = threading.Event()
                                    if k not in BLOCKING_CLIENTS: BLOCKING_CLIENTS[k] = []
                                    BLOCKING_CLIENTS[k].append((wait_bel, wait_box))
                            
                            if wait_bel:
                                w = wait_bel.wait(t_out) if t_out > 0 else (wait_bel.wait() or True)
                                with BLOCK_LOCK:
                                    if k in BLOCKING_CLIENTS: BLOCKING_CLIENTS[k] = [x for x in BLOCKING_CLIENTS[k] if x[0] != wait_bel]
                                target.sendall(f"*2\r\n${len(k)}\r\n{k}\r\n${len(wait_box[0])}\r\n{wait_box[0]}\r\n".encode() if wait_box else b"*-1\r\n")

                    except Exception as e:
                        target.sendall(f"-ERR {str(e)}\r\n".encode())

                    if is_exec: res_list.append(proxy.buf)

                if is_exec:
                    pk = f"*{len(res_list)}\r\n".encode()
                    for r in res_list: pk += r
                    connection.sendall(pk)

    except Exception: pass
    finally: connection.close()

def main():
    s = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle_client, args=(c,)).start()

if __name__ == "__main__": main()
