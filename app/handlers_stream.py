import time
import app.store as store
from app.protocol import format_xread_data

def handle_stream(cmd_name, cmd_parts, target, session=None):
    """Handles Redis Stream commands: XADD, XRANGE, XREAD, XGROUP, XREADGROUP, XACK."""
    from app.replication import propagate_command
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "XADD":
        key, entry_id = arg(1), arg(2)
        fields = {}
        for i in range(3, len(cmd_parts), 2):
            fields[cmd_parts[i]] = cmd_parts[i+1]
            
        if key not in store.DATA_STORE:
            store.DATA_STORE[key] = (store.Stream(), None)
        
        stream, _ = store.DATA_STORE[key]
        if not isinstance(stream, store.Stream):
            target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return True
            
        # Handle auto-generated IDs (*) and sequence numbers
        last_ms, last_seq = map(int, stream.last_id.split("-"))
        if entry_id == "*": entry_id = f"{int(time.time()*1000)}-*"
        
        if "-" in entry_id:
            ms_part, seq_part = entry_id.split("-")
            ms = int(ms_part)
            if seq_part == "*":
                seq = (last_seq + 1) if ms == last_ms else (1 if ms == 0 else 0)
            else:
                seq = int(seq_part)
            
            # Validation
            if ms < last_ms or (ms == last_ms and seq <= last_seq):
                if ms == 0 and seq == 0: target.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                else: target.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return True
            
            final_id = f"{ms}-{seq}"
            stream.add(final_id, fields)
            target.sendall(f"${len(final_id)}\r\n{final_id}\r\n".encode())
            propagate_command(cmd_parts)
        return True

    elif cmd_name == "XRANGE":
        key, start, end = arg(1), arg(2), arg(3)
        if key not in store.DATA_STORE:
            target.sendall(b"*0\r\n")
        else:
            stream, _ = store.DATA_STORE[key]
            # Suffix range handling
            if start == "-": start = "0-0"
            if end == "+": end = "9999999999999-999999"
            
            results = [e for e in stream.entries if start <= e[0] <= end]
            res = f"*{len(results)}\r\n"
            for eid, flds in results:
                res += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
                for fk, fv in flds.items(): res += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
            target.sendall(res.encode())
        return True

    elif cmd_name == "XREAD":
        # Format: XREAD [BLOCK timeout] STREAMS key [key ...] id [id ...]
        is_block = "BLOCK" in cmd_parts
        timeout = 0
        if is_block: timeout = int(cmd_parts[cmd_parts.index("BLOCK") + 1])
        
        s_idx = cmd_parts.index("STREAMS")
        num_streams = (len(cmd_parts) - 1 - s_idx) // 2
        keys = cmd_parts[s_idx+1 : s_idx+1+num_streams]
        ids = cmd_parts[s_idx+1+num_streams:]
        
        start_time = time.time()
        while True:
            read_data = []
            for k, last_id in zip(keys, ids):
                if k in store.DATA_STORE:
                    stream, _ = store.DATA_STORE[k]
                    # '$' means read only new entries
                    if last_id == "$": last_id = stream.last_id
                    new_entries = [e for e in stream.entries if e[0] > last_id]
                    if new_entries: read_data.append((k, new_entries))
            
            if read_data:
                target.sendall(format_xread_data(read_data).encode())
                return True
            
            if not is_block or (timeout > 0 and (time.time() - start_time) * 1000 > timeout):
                target.sendall(b"$-1\r\n")
                return True
            time.sleep(0.1)
    
    return False
