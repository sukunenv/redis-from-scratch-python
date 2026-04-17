import socket
import threading
import app.store as store
from app.protocol import parse_resp
import app.handlers as handlers

def propagate_command(cmd_p):
    """Mengirimkan perintah tulis ke semua Slave yang terhubung (Hanya untuk Master)"""
    if store.ROLE != "master": return
    if not store.REPLICAS: return
    
    # Format ulang perintah list menjadi RESP Array biner
    res = f"*{len(cmd_p)}\r\n"
    for arg in cmd_p:
        res += f"${len(str(arg))}\r\n{arg}\r\n"
    data = res.encode()
    
    # Update master offset
    store.MASTER_REPL_OFFSET += len(data)
    
    # Kirim ke semua Slave yang terdaftar secara aman
    with store.BLOCK_LOCK:
        for replica in store.REPLICAS:
            try: replica.sendall(data)
            except: pass

def initiate_handshake(master_host, master_port, server_port):
    """Logika jabat tangan Slave dengan Master (Hanya untuk Slave)"""
    try:
        master_conn = socket.create_connection((master_host, master_port))
        
        # 1. PING
        master_conn.sendall(b"*1\r\n$4\r\nPING\r\n")
        master_conn.recv(1024)
        
        # 2. REPLCONF listening-port
        cmd1 = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(server_port))}\r\n{server_port}\r\n"
        master_conn.sendall(cmd1.encode())
        master_conn.recv(1024)
        
        # 3. REPLCONF capa psync2
        cmd2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_conn.sendall(cmd2.encode())
        master_conn.recv(1024)
        
        # 4. PSYNC ? -1
        cmd3 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_conn.sendall(cmd3.encode())
        
        # 5. Pembersihan RDB (Presisi)
        buffer = b""
        while True:
            chunk = master_conn.recv(4096)
            if not chunk: break
            buffer += chunk
            
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
                        leftover = buffer[rdb_total_end:]
                        if leftover:
                            _process_slave_commands(leftover, master_conn)
                        break
                except: continue

        # 6. Loop Utama Propagasi Slave
        while True:
            data = master_conn.recv(4096)
            if not data: break
            _process_slave_commands(data, master_conn)

    except Exception as e:
        print(f"Koneksi Replikasi Terputus: {e}")

def _process_slave_commands(data, master_conn):
    """Fungsi pembantu untuk memproses perintah dari Master tanpa membalas (kecuali ACK)"""
    all_cmds = parse_resp(data)
    for c_cmd, cmd_len in all_cmds:
        if not c_cmd: continue
        
        class ReplicationProxy:
            def sendall(self, d):
                if b"REPLCONF" in d and b"ACK" in d:
                    master_conn.sendall(d)
        
        handlers.execute_command(c_cmd, ReplicationProxy())
        store.REPLICA_OFFSET += cmd_len
