import socket
import threading
import app.store as store
from app.protocol import parse_resp
import app.handlers as handlers

def propagate_command(cmd_parts):
    """Propagates a write command to all connected replicas (Master only)."""
    if store.ROLE != "master": return
    if not store.REPLICAS: return
    
    # Format the command list into a binary RESP Array
    res = f"*{len(cmd_parts)}\r\n"
    for arg in cmd_parts:
        res += f"${len(str(arg))}\r\n{arg}\r\n"
    data = res.encode()
    
    # Update master replication offset
    store.MASTER_REPL_OFFSET += len(data)
    
    # Send to all registered replicas safely
    with store.BLOCK_LOCK:
        for replica in store.REPLICAS:
            try: replica.sendall(data)
            except: pass

def initiate_handshake(master_host, master_port, server_port):
    """Executes the replication handshake with the Master (Slave only)."""
    try:
        master_conn = socket.create_connection((master_host, master_port))
        
        # 1. PING: Verify connectivity
        master_conn.sendall(b"*1\r\n$4\r\nPING\r\n")
        master_conn.recv(1024)
        
        # 2. REPLCONF listening-port: Inform Master of our listening port
        cmd1 = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(server_port))}\r\n{server_port}\r\n"
        master_conn.sendall(cmd1.encode())
        master_conn.recv(1024)
        
        # 3. REPLCONF capa psync2: Inform Master of our capabilities
        cmd2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_conn.sendall(cmd2.encode())
        master_conn.recv(1024)
        
        # 4. PSYNC ? -1: Request full synchronization
        cmd3 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_conn.sendall(cmd3.encode())
        
        # 5. Handle FULLRESYNC and RDB file transmission
        buffer = b""
        while True:
            chunk = master_conn.recv(4096)
            if not chunk: break
            buffer += chunk
            
            # Check if we have received the full RDB payload
            if b"+" in buffer and b"$" in buffer:
                idx_resync = buffer.find(b"\r\n")
                if idx_resync == -1: continue
                idx_rdb_header = buffer.find(b"$", idx_resync)
                idx_rdb_len_end = buffer.find(b"\r\n", idx_rdb_header)
                if idx_rdb_len_end == -1: continue
                
                try:
                    rdb_len = int(buffer[idx_rdb_header+1:idx_rdb_len_end])
                    rdb_total_end = idx_rdb_len_end + 2 + rdb_len
                    if len(buffer) >= rdb_total_end:
                        # Process any commands sent immediately after the RDB file
                        leftover = buffer[rdb_total_end:]
                        if leftover:
                            _process_slave_commands(leftover, master_conn)
                        break
                except: continue

        # 6. Main Propagation Loop: Receive and process Master's write stream
        while True:
            data = master_conn.recv(4096)
            if not data: break
            _process_slave_commands(data, master_conn)

    except Exception as e:
        print(f"Replication connection closed: {e}")

def _process_slave_commands(data, master_conn):
    """Processes commands from the Master without responding (except for ACK)."""
    all_cmds = parse_resp(data)
    for cmd_parts, cmd_len in all_cmds:
        if not cmd_parts: continue
        
        class ReplicationProxy:
            """Proxy to intercept and redirect REPLCONF ACK responses to the Master."""
            def sendall(self, d):
                if b"REPLCONF" in d and b"ACK" in d:
                    master_conn.sendall(d)
        
        handlers.execute_command(cmd_parts, ReplicationProxy())
        store.REPLICA_OFFSET += cmd_len
