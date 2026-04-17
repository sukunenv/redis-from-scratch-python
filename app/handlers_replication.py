import time
import app.store as store

def handle_replication(c, cmd_p, target):
    """Menangani perintah-perintah replikasi (INFO, REPLCONF, PSYNC, WAIT)"""
    from app.replication import propagate_command
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "INFO":
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
        return True

    elif c == "REPLCONF":
        if arg(1) and arg(1).upper() == "GETACK":
            target.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(store.REPLICA_OFFSET))}\r\n{store.REPLICA_OFFSET}\r\n".encode())
        elif arg(1) and arg(1).upper() == "ACK":
            try:
                off = int(arg(2))
                with store.BLOCK_LOCK: store.REPLICA_OFFSETS[target] = off
            except: pass
        else: target.sendall(b"+OK\r\n")
        return True

    elif c == "PSYNC":
        res = f"+FULLRESYNC {store.MASTER_REPLID} 0\r\n"
        target.sendall(res.encode())
        rdb_bin = bytes.fromhex(store.EMPTY_RDB_HEX)
        target.sendall(f"${len(rdb_bin)}\r\n".encode() + rdb_bin)
        with store.BLOCK_LOCK:
            if target not in store.REPLICAS: store.REPLICAS.append(target)
            store.REPLICA_OFFSETS[target] = 0
        return True

    elif c == "WAIT":
        try:
            num_required = int(arg(1))
            timeout_ms = int(arg(2))
        except:
            target.sendall(b"-ERR invalid arguments\r\n")
            return True

        target_offset = store.MASTER_REPL_OFFSET
        def count_synced():
            with store.BLOCK_LOCK:
                return sum(1 for off in store.REPLICA_OFFSETS.values() if off >= target_offset)

        if target_offset == 0:
            target.sendall(f":{len(store.REPLICAS)}\r\n".encode())
            return True

        if count_synced() < num_required:
            propagate_command(["REPLCONF", "GETACK", "*"])
            start_wait = time.time()
            while (time.time() - start_wait) < (timeout_ms / 1000.0):
                if count_synced() >= num_required: break
                time.sleep(0.01)

        target.sendall(f":{count_synced()}\r\n".encode())
        return True
    
    return False
