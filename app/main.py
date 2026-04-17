import socket    # Alat untuk membuat koneksi jaringan
import threading  # Alat untuk melayani banyak klien sekaligus
import time       # Alat untuk melihat jam/waktu sekarang

# Ini adalah "Gudang Data" kita.
# Isinya: { "kunci": (nilai, waktu_basi) }
DATA_STORE = {}

# === Class khusus untuk membedakan Stream dengan List/String biasa ===
class Stream:
    def __init__(self):
        self.entries = []

# === Peralatan untuk Blocking (BLPOP / XREAD) ===
BLOCK_LOCK = threading.Lock()
BLOCKING_CLIENTS = {}
STREAM_BLOCKING_CLIENTS = {}

def handle_client(connection):
    # Fungsi ini dijalankan untuk setiap klien yang terhubung
    try:
        # === State untuk Transaksi (MULTI/EXEC) ===
        is_transaction_active = False
        transaction_queue = []

        while True:
            # Mendengarkan pesan dari klien
            data = connection.recv(1024)
            if not data:
                break

            # Parse data RESP
            parts = data.decode().split("\r\n")
            if len(parts) < 3:
                continue
                
            command = parts[2].upper()

            # --- Logika Antrean Transaksi (Queueing) ---
            if is_transaction_active and command not in ["EXEC", "DISCARD"]:
                transaction_queue.append(parts)
                connection.sendall(b"+QUEUED\r\n")
                continue

            # Tentukan perintah apa saja yang akan dijalankan
            commands_to_run = []
            sedang_exec = False

            if command == "EXEC":
                if not is_transaction_active:
                    connection.sendall(b"-ERR EXEC without MULTI\r\n")
                    continue
                sedang_exec = True
                is_transaction_active = False
                commands_to_run = list(transaction_queue)
                transaction_queue = []
            else:
                commands_to_run = [parts]

            # Penampung balasan jika ini adalah EXEC
            exec_responses = []

            # === LOOP EKSEKUSI PERINTAH ===
            for p in commands_to_run:
                # Gunakan proxy connection jika sedang EXEC agar balasan tidak langsung terkirim
                current_conn = connection
                if sedang_exec:
                    class ProxyConn:
                        def __init__(self): self.output = b""
                        def sendall(self, d): self.output += d
                    proxy = ProxyConn()
                    current_conn = proxy

                # Ambil perintah lokal dari p
                cmd = p[2].upper()

                # ─────────────────────────────────────────
                # PERINTAH: PING
                # ─────────────────────────────────────────
                if cmd == "PING":
                    current_conn.sendall(b"+PONG\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: ECHO
                # ─────────────────────────────────────────
                elif cmd == "ECHO":
                    payload = p[4]
                    current_conn.sendall(f"${len(payload)}\r\n{payload}\r\n".encode())

                # ─────────────────────────────────────────
                # PERINTAH: SET
                # ─────────────────────────────────────────
                elif cmd == "SET":
                    key, value = p[4], p[6]
                    expiry = None
                    if len(p) > 8 and p[8].upper() == "PX":
                        expiry = time.time() + (int(p[10]) / 1000.0)
                    DATA_STORE[key] = (value, expiry)
                    current_conn.sendall(b"+OK\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: GET
                # ─────────────────────────────────────────
                elif cmd == "GET":
                    key = p[4]
                    if key in DATA_STORE:
                        val, exp = DATA_STORE[key]
                        if exp and time.time() > exp:
                            del DATA_STORE[key]
                            current_conn.sendall(b"$-1\r\n")
                        else:
                            current_conn.sendall(f"${len(val)}\r\n{val}\r\n".encode())
                    else:
                        current_conn.sendall(b"$-1\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: INCR
                # ─────────────────────────────────────────
                elif cmd == "INCR":
                    key = p[4]
                    if key in DATA_STORE:
                        val, exp = DATA_STORE[key]
                        try:
                            num = int(val) + 1
                            DATA_STORE[key] = (str(num), exp)
                            current_conn.sendall(f":{num}\r\n".encode())
                        except ValueError:
                            current_conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                    else:
                        DATA_STORE[key] = ("1", None)
                        current_conn.sendall(b":1\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: MULTI
                # ─────────────────────────────────────────
                elif cmd == "MULTI":
                    is_transaction_active = True
                    current_conn.sendall(b"+OK\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: TYPE
                # ─────────────────────────────────────────
                elif cmd == "TYPE":
                    key = p[4]
                    if key in DATA_STORE:
                        d, exp = DATA_STORE[key]
                        if exp and time.time() > exp:
                            del DATA_STORE[key]
                            current_conn.sendall(b"+none\r\n")
                        else:
                            if isinstance(d, str): current_conn.sendall(b"+string\r\n")
                            elif isinstance(d, list): current_conn.sendall(b"+list\r\n")
                            elif isinstance(d, Stream): current_conn.sendall(b"+stream\r\n")
                            else: current_conn.sendall(b"+none\r\n")
                    else:
                        current_conn.sendall(b"+none\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: XADD
                # ─────────────────────────────────────────
                elif cmd == "XADD":
                    key, entry_id = p[4], p[6]
                    # Logic ID generation (disingkat untuk efisiensi ruang)
                    if entry_id == "*":
                        ms = int(time.time() * 1000)
                        if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and DATA_STORE[key][0].entries:
                            l_id = DATA_STORE[key][0].entries[-1][0]
                            l_ms, l_seq = map(int, l_id.split("-"))
                            seq = l_seq + 1 if ms == l_ms else 0
                            ms = max(ms, l_ms)
                            entry_id = f"{ms}-{seq}"
                        else: entry_id = f"{ms}-0"
                    elif entry_id.endswith("-*"):
                        ms = int(entry_id.split("-")[0])
                        if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and DATA_STORE[key][0].entries:
                            l_id = DATA_STORE[key][0].entries[-1][0]
                            l_ms, l_seq = map(int, l_id.split("-"))
                            seq = l_seq + 1 if ms == l_ms else (0 if ms > 0 else 1)
                            entry_id = f"{ms}-{seq}"
                        else: entry_id = f"{ms}-{0 if ms > 0 else 1}"

                    if entry_id == "0-0":
                        current_conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    else:
                        flds = {}
                        for i in range(8, len(p) - 1, 4): flds[p[i]] = p[i+2]
                        valid = True
                        if key in DATA_STORE:
                            s, _ = DATA_STORE[key]
                            if isinstance(s, Stream) and s.entries:
                                l_ms, l_seq = map(int, s.entries[-1][0].split("-"))
                                n_ms, n_seq = map(int, entry_id.split("-"))
                                if n_ms < l_ms or (n_ms == l_ms and n_seq <= l_seq): valid = False
                            if valid and isinstance(s, Stream): s.entries.append((entry_id, flds))
                        else:
                            s = Stream()
                            s.entries.append((entry_id, flds))
                            DATA_STORE[key] = (s, None)
                        
                        if valid:
                            current_conn.sendall(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())
                            with BLOCK_LOCK:
                                if key in STREAM_BLOCKING_CLIENTS:
                                    for bel, _ in STREAM_BLOCKING_CLIENTS[key]: bel.set()
                        else:
                            current_conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: XRANGE
                # ─────────────────────────────────────────
                elif cmd == "XRANGE":
                    key, start, end = p[4], p[6], p[8]
                    def parse_id(rid, dflt_seq):
                        if rid == "-": return 0, 0
                        if rid == "+": return float('inf'), float('inf')
                        parts = rid.split("-")
                        return int(parts[0]), (int(parts[1]) if len(parts) > 1 else dflt_seq)
                    s_ms, s_seq = parse_id(start, 0)
                    e_ms, e_seq = parse_id(end, float('inf'))
                    res = []
                    if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream):
                        for eid, flds in DATA_STORE[key][0].entries:
                            ms, seq = map(int, eid.split("-"))
                            if (ms > s_ms or (ms == s_ms and seq >= s_seq)) and (ms < e_ms or (ms == e_ms and seq <= e_seq)):
                                res.append((eid, flds))
                    out = f"*{len(res)}\r\n"
                    for eid, flds in res:
                        out += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
                        for fk, fv in flds.items(): out += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
                    current_conn.sendall(out.encode())

                # ─────────────────────────────────────────
                # PERINTAH: XREAD
                # ─────────────────────────────────────────
                elif cmd == "XREAD":
                    up = [x.upper() for x in p]
                    b_ms = int(p[up.index("BLOCK")+2]) if "BLOCK" in up else -1
                    ks_idx = up.index("STREAMS")
                    args = p[ks_idx+2 : len(p)-1]
                    num = len(args)//2
                    ks, rids = args[:num], args[num:]
                    ids = []
                    for k, rid in zip(ks, rids):
                        if rid == "$" and k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream) and DATA_STORE[k][0].entries:
                            ids.append(DATA_STORE[k][0].entries[-1][0])
                        else: ids.append("0-0" if rid == "$" else rid)
                    
                    def fetch(k_list, id_list):
                        found_data = []
                        for k, bid in zip(k_list, id_list):
                            bms, bseq = map(int, bid.split("-"))
                            if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream):
                                matches = [(eid, f) for eid, f in DATA_STORE[k][0].entries if int(eid.split("-")[0]) > bms or (int(eid.split("-")[0]) == bms and int(eid.split("-")[1]) > bseq)]
                                if matches: found_data.append((k, matches))
                        return found_data

                    def pack(data):
                        o = f"*{len(data)}\r\n"
                        for k, ents in data:
                            o += f"*2\r\n${len(k)}\r\n{k}\r\n*{len(ents)}\r\n"
                            for eid, flds in ents:
                                o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
                                for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
                        return o

                    initial = fetch(ks, ids)
                    if initial: current_conn.sendall(pack(initial).encode())
                    elif b_ms >= 0:
                        bel = threading.Event()
                        with BLOCK_LOCK:
                            for k, bid in zip(ks, ids):
                                if k not in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[k] = []
                                STREAM_BLOCKING_CLIENTS[k].append((bel, bid))
                        woke = bel.wait(b_ms/1000.0) if b_ms > 0 else (bel.wait() or True)
                        with BLOCK_LOCK:
                            for k in ks:
                                if k in STREAM_BLOCKING_CLIENTS: STREAM_BLOCKING_CLIENTS[k] = [x for x in STREAM_BLOCKING_CLIENTS[k] if x[0] != bel]
                        final = fetch(ks, ids) if woke else None
                        current_conn.sendall(pack(final).encode() if final else b"*-1\r\n")
                    else: current_conn.sendall(b"*-1\r\n")

                # ─────────────────────────────────────────
                # PERINTAH: RPUSH / LPUSH
                # ─────────────────────────────────────────
                elif cmd in ["RPUSH", "LPUSH"]:
                    key = p[4]
                    els = [p[i] for i in range(6, len(p)-1, 2)]
                    if cmd == "LPUSH": els = list(reversed(els))
                    if key in DATA_STORE and isinstance(DATA_STORE[key][0], list):
                        l, ex = DATA_STORE[key]
                        if cmd == "RPUSH": l.extend(els)
                        else: l = els + l
                        DATA_STORE[key] = (l, ex)
                    else: DATA_STORE[key] = (els, None)
                    current_conn.sendall(f":{len(DATA_STORE[key][0])}\r\n".encode())
                    with BLOCK_LOCK:
                        while key in BLOCKING_CLIENTS and BLOCKING_CLIENTS[key] and DATA_STORE[key][0]:
                            it = DATA_STORE[key][0].pop(0)
                            bl, box = BLOCKING_CLIENTS[key].pop(0)
                            box.append(it)
                            bl.set()

                # ─────────────────────────────────────────
                # PERINTAH: LRANGE / LPOP / LLEN / BLPOP
                # ─────────────────────────────────────────
                elif cmd == "LRANGE":
                    key, start, stop = p[4], int(p[6]), int(p[8])
                    if key in DATA_STORE and isinstance(DATA_STORE[key][0], list):
                        l = DATA_STORE[key][0]
                        if start < 0: start += len(l)
                        if stop < 0: stop += len(l)
                        slice_obj = l[max(0, start):stop+1]
                        o = f"*{len(slice_obj)}\r\n"
                        for x in slice_obj: o += f"${len(x)}\r\n{x}\r\n"
                        current_conn.sendall(o.encode())
                    else: current_conn.sendall(b"*0\r\n")

                elif cmd == "LLEN":
                    key = p[4]
                    size = len(DATA_STORE[key][0]) if key in DATA_STORE and isinstance(DATA_STORE[key][0], list) else 0
                    current_conn.sendall(f":{size}\r\n".encode())

                elif cmd == "LPOP":
                    key = p[4]
                    cnt = int(p[6]) if len(p) > 6 and p[5].startswith("$") else None
                    if key in DATA_STORE and isinstance(DATA_STORE[key][0], list) and DATA_STORE[key][0]:
                        l, ex = DATA_STORE[key]
                        if cnt:
                            tkn, rem = l[:cnt], l[cnt:]
                            DATA_STORE[key] = (rem, ex)
                            o = f"*{len(tkn)}\r\n"
                            for x in tkn: o += f"${len(x)}\r\n{x}\r\n"
                            current_conn.sendall(o.encode())
                        else:
                            it = l.pop(0)
                            current_conn.sendall(f"${len(it)}\r\n{it}\r\n".encode())
                    else: current_conn.sendall(b"$-1\r\n")

                elif cmd == "BLPOP":
                    key, tout = p[4], float(p[6])
                    with BLOCK_LOCK:
                        if key in DATA_STORE and isinstance(DATA_STORE[key][0], list) and DATA_STORE[key][0]:
                            it = DATA_STORE[key][0].pop(0)
                            current_conn.sendall(f"*2\r\n${len(key)}\r\n{key}\r\n${len(it)}\r\n{it}\r\n".encode())
                        else:
                            bl, box = threading.Event(), []
                            if key not in BLOCKING_CLIENTS: BLOCKING_CLIENTS[key] = []
                            BLOCKING_CLIENTS[key].append((bl, box))
                            # Unlock while waiting
                            BLOCK_LOCK.release()
                            w = bl.wait(tout) if tout > 0 else (bl.wait() or True)
                            BLOCK_LOCK.acquire()
                            if key in BLOCKING_CLIENTS: BLOCKING_CLIENTS[key] = [x for x in BLOCKING_CLIENTS[key] if x[0] != bl]
                            current_conn.sendall(f"*2\r\n${len(key)}\r\n{key}\r\n${len(box[0])}\r\n{box[0]}\r\n".encode() if box else b"*-1\r\n")

                # Jika sedang EXEC, simpan hasil proxy ke daftar
                if sedang_exec:
                    responses_for_exec.append(proxy.output)

            # Jika tadi EXEC, kirim semua gabungan jawaban
            if sedang_exec:
                final_packet = f"*{len(responses_for_exec)}\r\n".encode()
                for r in responses_for_exec: final_packet += r
                connection.sendall(final_packet)

    except Exception:
        pass
    finally:
        connection.close()

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,)).start()

if __name__ == "__main__":
    main()
