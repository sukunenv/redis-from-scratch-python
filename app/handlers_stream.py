import time
import app.store as store
from app.protocol import format_xread_data

def handle_stream(c, cmd_p, target):
    """Menangani perintah Stream (XADD, XRANGE, XREAD)"""
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "XADD":
        k, eid = arg(1), arg(2)
        if eid == "*":
            ms = int(time.time() * 1000)
            if k in store.DATA_STORE and isinstance(store.DATA_STORE[k][0], store.Stream) and store.DATA_STORE[k][0].entries:
                l_id = store.DATA_STORE[k][0].entries[-1][0]
                l_ms, l_seq = map(int, l_id.split("-"))
                seq = l_seq + 1 if ms == l_ms else 0
                ms = max(ms, l_ms); eid = f"{ms}-{seq}"
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
            if k not in store.DATA_STORE: store.DATA_STORE[k] = (store.Stream(), None)
            s, _ = store.DATA_STORE[k]
            if s.add_entry(eid, flds):
                store.touch_key(k)
                target.sendall(f"${len(eid)}\r\n{eid}\r\n".encode())
            else: target.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        return True

    elif c == "XRANGE":
        k, start, end = arg(1), arg(2), arg(3)
        if k not in store.DATA_STORE: target.sendall(b"*0\r\n")
        else:
            s, _ = store.DATA_STORE[k]
            entries = s.get_range(start, end)
            res = f"*{len(entries)}\r\n"
            for eid, flds in entries:
                res += f"*2\r\n${len(eid)}\r\n{eid}\r\n"
                res += f"*{len(flds)*2}\r\n"
                for fk, fv in flds.items(): res += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
            target.sendall(res.encode())
        return True

    elif c == "XREAD":
        # Logika XREAD (bisa blocking atau non-blocking)
        streams_idx = cmd_p.index("streams")
        num_keys = (len(cmd_p) - streams_idx - 1) // 2
        keys = cmd_p[streams_idx+1 : streams_idx+1+num_keys]
        ids = cmd_p[streams_idx+1+num_keys:]
        
        # Selesaikan dulu ID "$" di awal agar tidak berubah-ubah saat looping
        resolved_ids = []
        for i in range(num_keys):
            k, sid = keys[i], ids[i]
            if sid == "$":
                if k in store.DATA_STORE:
                    s, _ = store.DATA_STORE[k]
                    resolved_ids.append(s.entries[-1][0] if s.entries else "0-0")
                else: resolved_ids.append("0-0")
            else: resolved_ids.append(sid)

        block_ms = None
        if "block" in cmd_p: block_ms = int(cmd_p[cmd_p.index("block") + 1])

        def get_results():
            res = []
            for i in range(num_keys):
                k, sid = keys[i], resolved_ids[i]
                if k in store.DATA_STORE:
                    s, _ = store.DATA_STORE[k]
                    entries = s.get_range(sid, "+", exclusive_start=True)
                    if entries: res.append((k, entries))
            return res

        results = get_results()
        if not results and block_ms is not None:
            start_wait = time.time()
            while not results:
                if block_ms > 0 and (time.time() - start_wait) * 1000 > block_ms: break
                time.sleep(0.1); results = get_results()
        
        if not results: target.sendall(b"*-1\r\n")
        else: target.sendall(format_xread_data(results).encode())
        return True
    
    return False
