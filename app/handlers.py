import time
import threading
import app.store as store
from app.protocol import format_xread_data

def propagate_command(cmd_p):
    """Mengirimkan perintah tulis ke semua Slave yang terhubung"""
    # HANYA MASTER yang boleh melakukan propagasi
    if store.ROLE != "master": return
    if not store.REPLICAS: return
    # Format ulang perintah list menjadi RESP Array biner
    res = f"*{len(cmd_p)}\r\n"
    for arg in cmd_p:
        res += f"${len(str(arg))}\r\n{arg}\r\n"
    data = res.encode()
    # Kirim ke semua Slave yang terdaftar secara aman
    with store.BLOCK_LOCK:
        for replica in store.REPLICAS:
            try: replica.sendall(data)
            except: pass

def execute_command(cmd_p, target):
    """
    Eksekutor Perintah: Di sinilah otak dari setiap perintah Redis berada.
    Setiap blok 'elif' menangani satu jenis perintah spesifik.
    """
    try:
        c = cmd_p[0].upper()
        # Fungsi pembantu untuk mengambil argumen secara aman
        def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

        if c == "PING":
            target.sendall(b"+PONG\r\n")

        elif c == "INFO":
            # Memberikan info statistik (tahap awal: replication)
            section = arg(1)
            if section and section.lower() == "replication":
                lines = [
                    f"role:{store.ROLE}",
                    f"master_replid:{store.MASTER_REPLID}",
                    f"master_repl_offset:{store.MASTER_REPL_OFFSET}"
                ]
                res = "\r\n".join(lines)
                target.sendall(f"${len(res)}\r\n{res}\r\n".encode())
            else: target.sendall(b"$-1\r\n")

        elif c == "REPLCONF":
            # Perintah konfigurasi replikasi
            if arg(1) and arg(1).upper() == "GETACK":
                # Slave merespons dengan jumlah byte yang sudah diproses
                target.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(store.REPLICA_OFFSET))}\r\n{store.REPLICA_OFFSET}\r\n".encode())
            else:
                # Master cukup menjawab OK untuk konfigurasi awal
                target.sendall(b"+OK\r\n")

        elif c == "PSYNC":
            # 1. Jawab dengan FULLRESYNC
            res = f"+FULLRESYNC {store.MASTER_REPLID} 0\r\n"
            target.sendall(res.encode())
            
            # 2. Kirim file RDB kosong (Format: $<panjang>\r\n<isi_biner>)
            rdb_bin = bytes.fromhex(store.EMPTY_RDB_HEX)
            header = f"${len(rdb_bin)}\r\n".encode()
            target.sendall(header + rdb_bin)
            
            # 3. DAFTARKAN SLAVE: Mulai sekarang koneksi ini akan menerima update data
            with store.BLOCK_LOCK:
                if target not in store.REPLICAS:
                    store.REPLICAS.append(target)

        elif c == "ECHO":
            val = arg(1) or ""
            target.sendall(f"${len(val)}\r\n{val}\r\n".encode())

        elif c == "SET":
            # Menyimpan data dengan opsi PX (kedaluwarsa dalam milidetik)
            k, v = arg(1), arg(2)
            exp = None
            if len(cmd_p) > 4 and cmd_p[3].upper() == "PX":
                exp = time.time() + (int(cmd_p[4]) / 1000.0)
            store.DATA_STORE[k] = (v, exp)
            store.touch_key(k) # Beritahu sistem WATCH kalau kunci ini berubah
            target.sendall(b"+OK\r\n")
            
            # 4. PROPAGASI: Kirim perintah SET ini ke semua Slave
            propagate_command(cmd_p)

        elif c == "GET":
            # Mengambil data, cek dulu apakah sudah basi (expired)
            k = arg(1)
            if k in store.DATA_STORE:
                val, ex = store.DATA_STORE[k]
                if ex and time.time() > ex:
                    del store.DATA_STORE[k]
                    store.touch_key(k)
                    target.sendall(b"$-1\r\n")
                elif not isinstance(val, str):
                    target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                else:
                    target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
            else: target.sendall(b"$-1\r\n")

        elif c == "INCR":
            # Menambah nilai angka sebesar 1
            k = arg(1)
            if k in store.DATA_STORE:
                v, ex = store.DATA_STORE[k]
                try:
                    num = int(v) + 1
                    store.DATA_STORE[k] = (str(num), ex)
                    store.touch_key(k)
                    target.sendall(f":{num}\r\n".encode())
                except (ValueError, TypeError):
                    target.sendall(b"-ERR value is not an integer or out of range\r\n")
            else:
                store.DATA_STORE[k] = ("1", None)
                store.touch_key(k)
                target.sendall(b":1\r\n")
                # PROPAGASI
                propagate_command(cmd_p)

        elif c == "TYPE":
            # Mengecek tipe data (string, list, atau stream)
            k = arg(1)
            if k not in store.DATA_STORE: target.sendall(b"+none\r\n")
            else:
                v, ex = store.DATA_STORE[k]
                if ex and time.time() > ex:
                    del store.DATA_STORE[k]
                    store.touch_key(k)
                    target.sendall(b"+none\r\n")
                else:
                    if isinstance(v, str): target.sendall(b"+string\r\n")
                    elif isinstance(v, list): target.sendall(b"+list\r\n")
                    elif isinstance(v, store.Stream): target.sendall(b"+stream\r\n")
                    else: target.sendall(b"+none\r\n")

        elif c == "XADD":
            # Menambah data ke Stream dengan ID unik
            k, eid = arg(1), arg(2)
            # Logika pembuatan ID otomatis (* atau -*)
            if eid == "*":
                ms = int(time.time() * 1000)
                if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], store.Stream) and store.DATA_STORE[k][0].entries:
                    l_id = store.DATA_STORE[k][0].entries[-1][0]
                    l_ms, l_seq = map(int, l_id.split("-"))
                    seq = l_seq + 1 if ms == l_ms else 0
                    ms = max(ms, l_ms)
                    eid = f"{ms}-{seq}"
                else: eid = f"{ms}-0"
            elif eid.endswith("-*"):
                ms = int(eid.split("-")[0])
                if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], store.Stream) and store.DATA_STORE[k][0].entries:
                    l_id = store.DATA_STORE[k][0].entries[-1][0]
                    l_ms, l_seq = map(int, l_id.split("-"))
                    seq = l_seq + 1 if ms == l_ms else (0 if ms > 0 else 1)
                    eid = f"{ms}-{seq}"
                else: eid = f"{ms}-{0 if ms > 0 else 1}"

            if eid == "0-0": target.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            else:
                flds = {}
                for idx in range(3, len(cmd_p)-1, 2): flds[cmd_p[idx]] = cmd_p[idx+1]
                valid = True
                if k in store.DATA_STORE:
                    s, _ = store.DATA_STORE[k]
                    if isinstance(s, store.Stream) and s.entries:
                        lm, ls = map(int, s.entries[-1][0].split("-"))
                        nm, ns = map(int, eid.split("-"))
                        # ID baru tidak boleh lebih kecil dari ID terakhir
                        if nm < lm or (nm == lm and ns <= ls): valid = False
                    if valid and isinstance(s, store.Stream): s.entries.append((eid, flds))
                else:
                    s = store.Stream()
                    s.entries.append((eid, flds))
                    store.DATA_STORE[k] = (s, None)
                if valid:
                    store.touch_key(k)
                    target.sendall(f"${len(eid)}\r\n{eid}\r\n".encode())
                    # PROPAGASI
                    propagate_command(cmd_p)
                    with store.BLOCK_LOCK:
                        if k in store.STREAM_BLOCKING_CLIENTS:
                            # Bangunkan klien yang sedang menunggu data (XREAD BLOCK)
                            for bel, _ in store.STREAM_BLOCKING_CLIENTS[k]: bel.set()
                else: target.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

        elif c == "XRANGE":
            # Mengambil daftar entri stream dalam rentang ID tertentu
            k, start, end = arg(1), arg(2), arg(3)
            def p_id(rid, dflt):
                if rid == "-": return 0, 0
                if rid == "+": return float('inf'), float('inf')
                ps = rid.split("-")
                return int(ps[0]), (int(ps[1]) if len(ps)>1 else dflt)
            sm, ss = p_id(start, 0)
            em, es = p_id(end, float('inf'))
            res = []
            if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], store.Stream):
                for eid, flds in store.DATA_STORE[k][0].entries:
                    m, s = map(int, eid.split("-"))
                    if (m > sm or (m == sm and s >= ss)) and (m < em or (m == em and s <= es)):
                        res.append((eid, flds))
            o = f"*{len(res)}\r\n"
            for eid, flds in res:
                o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
                for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
            target.sendall(o.encode())

        elif c == "XREAD":
            # Membaca stream, bisa menunggu (BLOCK) jika data belum ada
            u_p = [x.upper() for x in cmd_p]
            b_ms = int(cmd_p[u_p.index("BLOCK")+1]) if "BLOCK" in u_p else -1
            s_idx = u_p.index("STREAMS")
            args = cmd_p[s_idx+1:]
            n = len(args)//2
            ks, rids = args[:n], args[n:]
            ids = []
            for kk, rid in zip(ks, rids):
                if rid == "$" and kk in store.DATA_STORE and isinstance(store.DATA_STORE[kk][0], store.Stream) and store.DATA_STORE[kk][0].entries:
                    ids.append(store.DATA_STORE[kk][0].entries[-1][0])
                else: ids.append("0-0" if rid == "$" else rid)

            def fetch():
                fnd = []
                for kk, bid in zip(ks, ids):
                    bm, bs = map(int, bid.split("-"))
                    if kk in store.DATA_STORE and isinstance(store.DATA_STORE[kk][0], store.Stream):
                        mtch = [(eid, f) for eid, f in store.DATA_STORE[kk][0].entries if int(eid.split("-")[0]) > bm or (int(eid.split("-")[0]) == bm and int(eid.split("-")[1]) > bs)]
                        if mtch: fnd.append((kk, mtch))
                return fnd

            init = fetch()
            if init: target.sendall(format_xread_data(init).encode())
            elif b_ms >= 0:
                bel = threading.Event()
                with store.BLOCK_LOCK:
                    for kk, bid in zip(ks, ids):
                        if kk not in store.STREAM_BLOCKING_CLIENTS: store.STREAM_BLOCKING_CLIENTS[kk] = []
                        store.STREAM_BLOCKING_CLIENTS[kk].append((bel, bid))
                woke = bel.wait(b_ms/1000.0) if b_ms > 0 else (bel.wait() or True)
                with store.BLOCK_LOCK:
                    for kk in ks:
                        if kk in store.STREAM_BLOCKING_CLIENTS: store.STREAM_BLOCKING_CLIENTS[kk] = [x for x in store.STREAM_BLOCKING_CLIENTS[kk] if x[0] != bel]
                fin = fetch() if woke else None
                target.sendall(format_xread_data(fin).encode() if fin else b"*-1\r\n")
            else: target.sendall(b"*-1\r\n")

        elif c in ["RPUSH", "LPUSH"]:
            # Memasukkan elemen ke List (di ujung kanan atau kiri)
            k, items = arg(1), cmd_p[2:]
            if c == "LPUSH": items = list(reversed(items))
            if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], list):
                l, ex = store.DATA_STORE[k]
                if c == "RPUSH": l.extend(items)
                else: l = items + l
                store.DATA_STORE[k] = (l, ex)
            else: store.DATA_STORE[k] = (items, None)
            store.touch_key(k)
            target.sendall(f":{len(store.DATA_STORE[k][0])}\r\n".encode())
            # PROPAGASI
            propagate_command(cmd_p)
            # Bangunkan klien BLPOP yang sedang menunggu (Blocking)
            with store.BLOCK_LOCK:
                while k in store.BLOCKING_CLIENTS and store.BLOCKING_CLIENTS[k] and store.DATA_STORE[k][0]:
                    it = store.DATA_STORE[k][0].pop(0)
                    bl, box = store.BLOCKING_CLIENTS[k].pop(0)
                    box.append(it)
                    bl.set()

        elif c == "LRANGE":
            # Mengambil elemen list dalam rentang indeks tertentu
            k, st, sp = arg(1), int(arg(2)), int(arg(3))
            if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], list):
                l = store.DATA_STORE[k][0]
                if st < 0: st += len(l)
                if sp < 0: sp += len(l)
                sl = l[max(0, st):sp+1]
                o = f"*{len(sl)}\r\n"
                for x in sl: o += f"${len(x)}\r\n{x}\r\n"
                target.sendall(o.encode())
            else: target.sendall(b"*0\r\n")

        elif c == "LLEN":
            # Menghitung panjang List
            k = arg(1)
            size = len(store.DATA_STORE[k][0]) if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], list) else 0
            target.sendall(f":{size}\r\n".encode())

        elif c == "LPOP":
            # Mengambil elemen dari ujung kiri list
            k, cn = arg(1), (int(arg(2)) if len(cmd_p)>2 else None)
            if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], list) and store.DATA_STORE[k][0]:
                l, ex = store.DATA_STORE[k]
                if cn:
                    tk, rm = l[:cn], l[cn:]
                    store.DATA_STORE[k] = (rm, ex)
                    store.touch_key(k)
                    o = f"*{len(tk)}\r\n"
                    for x in tk: o += f"${len(x)}\r\n{x}\r\n"
                    target.sendall(o.encode())
                else:
                    it = l.pop(0)
                    store.touch_key(k)
                    target.sendall(f"${len(it)}\r\n{it}\r\n".encode())
                    # PROPAGASI
                    propagate_command(cmd_p)
            else: target.sendall(b"$-1\r\n")

        elif c == "BLPOP":
            # Versi blocking dari LPOP (menunggu jika list kosong)
            k, t_out = arg(1), float(arg(2))
            wait_bel, wait_box = None, []
            with store.BLOCK_LOCK:
                if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], list) and store.DATA_STORE[k][0]:
                    it = store.DATA_STORE[k][0].pop(0)
                    store.touch_key(k)
                    target.sendall(f"*2\r\n${len(k)}\r\n{k}\r\n${len(it)}\r\n{it}\r\n".encode())
                else:
                    wait_bel = threading.Event()
                    if k not in store.BLOCKING_CLIENTS: store.BLOCKING_CLIENTS[k] = []
                    store.BLOCKING_CLIENTS[k].append((wait_bel, wait_box))
            
            if wait_bel:
                w = wait_bel.wait(t_out) if t_out > 0 else (wait_bel.wait() or True)
                with store.BLOCK_LOCK:
                    if k in store.BLOCKING_CLIENTS: store.BLOCKING_CLIENTS[k] = [x for x in store.BLOCKING_CLIENTS[k] if x[0] != wait_bel]
                if wait_box: store.touch_key(k)
                target.sendall(f"*2\r\n${len(k)}\r\n{k}\r\n${len(wait_box[0])}\r\n{wait_box[0]}\r\n".encode() if wait_box else b"*-1\r\n")

        else:
            target.sendall(f"-ERR unknown command '{c}'\r\n".encode())

    except Exception as e:
        target.sendall(f"-ERR {str(e)}\r\n".encode())
