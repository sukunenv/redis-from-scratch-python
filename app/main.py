import socket
import threading
import time

# Gudang Data Utama
DATA_STORE = {}

class Stream:
    def __init__(self):
        self.entries = []

# Peralatan Sinkronisasi untuk Blocking Commands
BLOCK_LOCK = threading.Lock()
BLOCKING_CLIENTS = {}
STREAM_BLOCKING_CLIENTS = {}

def parse_resp(data):
    """
    Fungsi pembantu untuk memotong-motong data RESP yang masuk.
    Bisa menangani banyak perintah sekaligus dalam satu paket data.
    """
    if not data: return []
    lines = data.decode().split("\r\n")
    commands = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line:
            i += 1
            continue
        if line.startswith('*'):
            num_args = int(line[1:])
            cmd_parts = []
            i += 1
            for _ in range(num_args):
                # Lewati baris $panjang, ambil isinya
                if i + 1 < len(lines):
                    cmd_parts.append(lines[i+1])
                    i += 2
            commands.append(cmd_parts)
        else:
            i += 1
    return commands

def handle_client(connection):
    try:
        is_transaction_active = False
        transaction_queue = []

        while True:
            raw_data = connection.recv(4096)
            if not raw_data:
                break

            # Gunakan parser yang lebih kuat untuk memisahkan perintah
            all_commands = parse_resp(raw_data)
            
            for cmd_parts in all_commands:
                if not cmd_parts: continue
                
                command = cmd_parts[0].upper()

                # --- Logika Transaksi (MULTI/EXEC/DISCARD) ---
                if is_transaction_active and command not in ["EXEC", "DISCARD"]:
                    transaction_queue.append(cmd_parts)
                    connection.sendall(b"+QUEUED\r\n")
                    continue

                commands_to_execute = []
                is_exec_mode = False

                if command == "MULTI":
                    is_transaction_active = True
                    connection.sendall(b"+OK\r\n")
                    continue
                elif command == "DISCARD":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        is_transaction_active = False
                        transaction_queue = []
                        connection.sendall(b"+OK\r\n")
                    continue
                elif command == "EXEC":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    is_exec_mode = True
                    is_transaction_active = False
                    commands_to_execute = list(transaction_queue)
                    transaction_queue = []
                else:
                    commands_to_execute = [cmd_parts]

                # Kolektor balasan untuk mode EXEC
                exec_responses = []

                # === LOOP EKSEKUSI UTAMA ===
                for p in commands_to_execute:
                    # Proxy untuk menangkap output jika dalam mode EXEC
                    class Proxy:
                        def __init__(self): self.buf = b""
                        def sendall(self, d): self.buf += d
                    
                    target_conn = connection
                    if is_exec_mode:
                        proxy = Proxy()
                        target_conn = proxy

                    try:
                        c = p[0].upper()

                        if c == "PING":
                            target_conn.sendall(b"+PONG\r\n")

                        elif c == "ECHO":
                            val = p[1] if len(p) > 1 else ""
                            target_conn.sendall(f"${len(val)}\r\n{val}\r\n".encode())

                        elif c == "SET":
                            key, val = p[1], p[2]
                            exp = None
                            # SET k v PX 100
                            if len(p) > 4 and p[3].upper() == "PX":
                                exp = time.time() + (int(p[4]) / 1000.0)
                            DATA_STORE[key] = (val, exp)
                            target_conn.sendall(b"+OK\r\n")

                        elif c == "GET":
                            key = p[1]
                            if key in DATA_STORE:
                                v, ex = DATA_STORE[key]
                                if ex and time.time() > ex:
                                    del DATA_STORE[key]
                                    target_conn.sendall(b"$-1\r\n")
                                elif not isinstance(v, str):
                                    target_conn.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                else:
                                    target_conn.sendall(f"${len(v)}\r\n{v}\r\n".encode())
                            else:
                                target_conn.sendall(b"$-1\r\n")

                        elif c == "INCR":
                            key = p[1]
                            if key in DATA_STORE:
                                v, ex = DATA_STORE[key]
                                if not isinstance(v, str):
                                    target_conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                                else:
                                    try:
                                        num = int(v) + 1
                                        DATA_STORE[key] = (str(num), ex)
                                        target_conn.sendall(f":{num}\r\n".encode())
                                    except (ValueError, TypeError):
                                        target_conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                            else:
                                DATA_STORE[key] = ("1", None)
                                target_conn.sendall(b":1\r\n")

                        elif c == "TYPE":
                            key = p[1]
                            if key not in DATA_STORE:
                                target_conn.sendall(b"+none\r\n")
                            else:
                                d, ex = DATA_STORE[key]
                                if ex and time.time() > ex:
                                    del DATA_STORE[key]
                                    target_conn.sendall(b"+none\r\n")
                                else:
                                    if isinstance(d, str): target_conn.sendall(b"+string\r\n")
                                    elif isinstance(d, list): target_conn.sendall(b"+list\r\n")
                                    elif isinstance(d, Stream): target_conn.sendall(b"+stream\r\n")
                                    else: target_conn.sendall(b"+none\r\n")

                        elif c == "XADD":
                            key, eid = p[1], p[2]
                            # Auto-ID Logic
                            if eid == "*":
                                ms = int(time.time() * 1000)
                                if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and DATA_STORE[key][0].entries:
                                    l_id = DATA_STORE[key][0].entries[-1][0]
                                    l_ms, l_seq = map(int, l_id.split("-"))
                                    seq = l_seq + 1 if ms == l_ms else 0
                                    ms = max(ms, l_ms)
                                    eid = f"{ms}-{seq}"
                                else: eid = f"{ms}-0"
                            elif eid.endswith("-*"):
                                ms = int(eid.split("-")[0])
                                if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and DATA_STORE[key][0].entries:
                                    l_id = DATA_STORE[key][0].entries[-1][0]
                                    l_ms, l_seq = map(int, l_id.split("-"))
                                    seq = l_seq + 1 if ms == l_ms else (0 if ms > 0 else 1)
                                    eid = f"{ms}-{seq}"
                                else: eid = f"{ms}-{0 if ms > 0 else 1}"

                            if eid == "0-0":
                                target_conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                            else:
                                # Fields parsing
                                flds = {}
                                for idx in range(3, len(p) - 1, 2):
                                    flds[p[idx]] = p[idx+1]
                                
                                valid = True
                                if key in DATA_STORE:
                                    s, _ = DATA_STORE[key]
                                    if isinstance(s, Stream) and s.entries:
                                        l_ms, l_seq = map(int, s.entries[-1][0].split("-"))
                                        n_ms, n_seq = map(int, eid.split("-"))
                                        if n_ms < l_ms or (n_ms == l_ms and n_seq <= l_seq): valid = False
                                    if valid and isinstance(s, Stream): s.entries.append((eid, flds))
                                else:
                                    s = Stream()
                                    s.entries.append((eid, flds))
                                    DATA_STORE[key] = (s, None)
                                
                                if valid:
                                    target_conn.sendall(f"${len(eid)}\r\n{eid}\r\n".encode())
                                    with BLOCK_LOCK:
                                        if key in STREAM_BLOCKING_CLIENTS:
                                            for bel, _ in STREAM_BLOCKING_CLIENTS[key]: bel.set()
                                else:
                                    target_conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

                        elif c == "XREAD":
                            # Sederhanakan pencarian STREAMS dan BLOCK
                            try:
                                block_ms = -1
                                if "BLOCK" in [x.upper() for x in p]:
                                    block_ms = int(p[p.index("BLOCK") + 1] if "BLOCK" in p else p[[x.upper() for x in p].index("BLOCK") + 1])
                                
                                s_idx = -1
                                for i, v in enumerate(p):
                                    if v.upper() == "STREAMS":
                                        s_idx = i
                                        break
                                
                                remaining = p[s_idx+1:]
                                num_ks = len(remaining) // 2
                                ks, rids = remaining[:num_ks], remaining[num_ks:]
                                ids = []
                                for k, rid in zip(ks, rids):
                                    if rid == "$" and key in DATA_STORE and isinstance(DATA_STORE[k][0], Stream) and DATA_STORE[k][0].entries:
                                        ids.append(DATA_STORE[k][0].entries[-1][0])
                                    else: ids.append("0-0" if rid == "$" else rid)

                                def fetch_stream():
                                    res_st = []
                                    for k, bid in zip(ks, ids):
                                        bms, bseq = map(int, bid.split("-"))
                                        if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream):
                                            mtch = [(eid, f) for eid, f in DATA_STORE[k][0].entries if int(eid.split("-")[0]) > bms or (int(eid.split("-")[0]) == bms and int(eid.split("-")[1]) > bseq)]
                                            if mtch: res_st.append((k, mtch))
                                    return res_st

                                initial = fetch_stream()
                                if initial: target_conn.sendall(format_xread(initial).encode())
                                elif block_ms >= 0:
                                    bel = threading.Event()
                                    with BLOCK_LOCK:
                                        for k, bid in zip(ks, ids):
                                            if k not in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[k] = []
                                            STREAM_BLOCKING_CLIENTS[k].append((bel, bid))
                                    woke = bel.wait(block_ms/1000.0) if block_ms > 0 else (bel.wait() or True)
                                    with BLOCK_LOCK:
                                        for k in ks:
                                            if k in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[k] = [x for x in STREAM_BLOCKING_CLIENTS[k] if x[0] != bel]
                                    final = fetch_stream() if woke else None
                                    target_conn.sendall(format_xread(final).encode() if final else b"*-1\r\n")
                                else: target_conn.sendall(b"*-1\r\n")
                            except: target_conn.sendall(b"*-1\r\n")

                        elif c in ["RPUSH", "LPUSH"]:
                            key, items = p[1], p[2:]
                            if c == "LPUSH": items = list(reversed(items))
                            if key in DATA_STORE and isinstance(DATA_STORE[key][0], list):
                                l, ex = DATA_STORE[key]
                                if c == "RPUSH": l.extend(items)
                                else: l = items + l
                                DATA_STORE[key] = (l, ex)
                            else: DATA_STORE[key] = (items, None)
                            target_conn.sendall(f":{len(DATA_STORE[key][0])}\r\n".encode())
                            with BLOCK_LOCK:
                                while key in BLOCKING_CLIENTS and BLOCKING_CLIENTS[key] and DATA_STORE[key][0]:
                                    it = DATA_STORE[key][0].pop(0)
                                    bl, box = BLOCKING_CLIENTS[key].pop(0)
                                    box.append(it)
                                    bl.set()

                        elif c == "LRANGE":
                            key, start, stop = p[1], int(p[2]), int(p[3])
                            if key in DATA_STORE and isinstance(DATA_STORE[key][0], list):
                                l = DATA_STORE[key][0]
                                if start < 0: start += len(l)
                                if stop < 0: stop += len(l)
                                slc = l[max(0, start):stop+1]
                                o = f"*{len(slc)}\r\n"
                                for x in slc: o += f"${len(x)}\r\n{x}\r\n"
                                target_conn.sendall(o.encode())
                            else: target_conn.sendall(b"*0\r\n")

                        elif c == "LPOP":
                            key = p[1]
                            cnt = int(p[2]) if len(p) > 2 else None
                            if key in DATA_STORE and isinstance(DATA_STORE[key][0], list) and DATA_STORE[key][0]:
                                l, ex = DATA_STORE[key]
                                if cnt:
                                    tkn, rem = l[:cnt], l[cnt:]
                                    DATA_STORE[key] = (rem, ex)
                                    o = f"*{len(tkn)}\r\n"
                                    for x in tkn: o += f"${len(x)}\r\n{x}\r\n"
                                    target_conn.sendall(o.encode())
                                else:
                                    it = l.pop(0)
                                    target_conn.sendall(f"${len(it)}\r\n{it}\r\n".encode())
                            else: target_conn.sendall(b"$-1\r\n")

                    except Exception as e:
                        target_conn.sendall(f"-ERR {str(e)}\r\n".encode())

                    if is_exec_mode:
                        exec_responses.append(proxy.buf)

                # Hasil akhir EXEC
                if is_exec_mode:
                    packet = f"*{len(exec_responses)}\r\n".encode()
                    for r in exec_responses: packet += r
                    connection.sendall(packet)

    except Exception:
        pass
    finally:
        connection.close()

def format_xread(data):
    o = f"*{len(data)}\r\n"
    for k, ents in data:
        o += f"*2\r\n${len(k)}\r\n{k}\r\n*{len(ents)}\r\n"
        for eid, flds in ents:
            o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
            for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
    return o

def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__":
    main()
