import app.store as store
import base64

def handle_replication(cmd_name, cmd_parts, target, session=None):
    """Handles Replication commands: INFO, REPLCONF, PSYNC, WAIT."""
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "INFO":
        section = arg(1).lower() if arg(1) else "replication"
        if section == "replication":
            res = f"role:{store.ROLE}\r\n"
            res += f"master_replid:{store.MASTER_REPLID}\r\n"
            res += f"master_repl_offset:{store.MASTER_REPL_OFFSET}\r\n"
            target.sendall(f"${len(res)}\r\n{res}\r\n".encode())
        return True

    elif cmd_name == "REPLCONF":
        sub = arg(1).upper()
        if sub == "GETACK":
            # Master asking for ACK: send our current offset
            res = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(store.REPLICA_OFFSET))}\r\n{store.REPLICA_OFFSET}\r\n"
            target.sendall(res.encode())
        elif sub == "ACK":
            # Replica sending ACK: record it
            with store.BLOCK_LOCK:
                store.ACK_COUNT += 1
        else:
            target.sendall(b"+OK\r\n")
        return True

    elif cmd_name == "PSYNC":
        # Master response to replica's sync request
        res = f"+FULLRESYNC {store.MASTER_REPLID} 0\r\n"
        target.sendall(res.encode())
        
        # Send a minimal empty RDB file to initialize the replica
        empty_rdb = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbS+ZWR6AD2FjdXNlZC1tZW3CsM4IAPoIYW9mLWJhc2XAAf/vS7M6vM7dgrQ=")
        target.sendall(f"${len(empty_rdb)}\r\n".encode() + empty_rdb)
        
        # Register the new replica for command propagation
        with store.BLOCK_LOCK:
            store.REPLICAS.append(target)
        return True

    elif cmd_name == "WAIT":
        # Supports the WAIT command for synchronous replication
        num_replicas = int(arg(1))
        timeout = int(arg(2))
        
        # Simple implementation: return current ack count if offset hasn't changed
        if store.MASTER_REPL_OFFSET == 0:
            target.sendall(f":{len(store.REPLICAS)}\r\n".encode())
        else:
            # Placeholder for actual wait logic
            target.sendall(f":{min(num_replicas, len(store.REPLICAS))}\r\n".encode())
        return True

    return False
