import os
import sys
import socket
import threading

import app.store as store
from app.protocol import parse_resp
from app.handlers import execute_command
from app.replication import initiate_handshake
from app.rdb import load_rdb
from app.aof import init_aof, replay_aof

def setup_aof():
    """Early initialization of AOF directory and files to avoid race conditions."""
    _dir = os.getcwd()
    _appendonly = "no"
    _appenddirname = "appendonlydir"
    _appendfilename = "appendonly.aof"

    # Fast-track argument parsing for AOF setup
    for i in range(len(sys.argv) - 1):
        if sys.argv[i] == "--dir": _dir = sys.argv[i+1]
        elif sys.argv[i] == "--appendonly": _appendonly = sys.argv[i+1]
        elif sys.argv[i] == "--appenddirname": _appenddirname = sys.argv[i+1]
        elif sys.argv[i] == "--appendfilename": _appendfilename = sys.argv[i+1]

    if _appendonly.strip().lower() == "yes":
        aof_path = os.path.join(_dir, _appenddirname)
        os.makedirs(aof_path, exist_ok=True)
        
        # Ensure initial incremental AOF file exists
        aof_file = os.path.join(aof_path, f"{_appendfilename}.1.incr.aof")
        if not os.path.exists(aof_file):
            open(aof_file, 'a').close()
            
        # Create manifest file if it doesn't exist
        manifest_file = os.path.join(aof_path, f"{_appendfilename}.manifest")
        if not os.path.exists(manifest_file):
            with open(manifest_file, "w") as f:
                f.write(f"file {_appendfilename}.1.incr.aof seq 1 type i\n")

def handle_client(connection):
    """Handles incoming client connections and command execution."""
    try:
        session = {
            "is_transaction_active": False,
            "transaction_queue": [],
            "watched_keys": {},
            "subscribed_channels": set()
        }

        while True:
            raw_data = connection.recv(4096)
            if not raw_data:
                break

            all_cmds = parse_resp(raw_data)
            for cmd_parts, _ in all_cmds:
                if not cmd_parts:
                    continue
                
                cmd_name = cmd_parts[0].upper()

                # --- Transaction Handling ---
                if session["is_transaction_active"] and cmd_name not in ["EXEC", "DISCARD"]:
                    if cmd_name in ["WATCH", "UNWATCH"]:
                        connection.sendall(f"-ERR {cmd_name} inside MULTI is not allowed\r\n".encode())
                    else:
                        session["transaction_queue"].append(cmd_parts)
                        connection.sendall(b"+QUEUED\r\n")
                    continue

                # --- Subscribed Mode Filter ---
                if len(session["subscribed_channels"]) > 0:
                    allowed = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"]
                    if cmd_name not in allowed:
                        connection.sendall(f"-ERR Can't execute '{cmd_name.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode())
                        continue

                # --- Transaction Control Commands ---
                if cmd_name == "MULTI":
                    session["is_transaction_active"] = True
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "DISCARD":
                    if not session["is_transaction_active"]:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        session["is_transaction_active"] = False
                        session["transaction_queue"] = []
                        session["watched_keys"] = {}
                        connection.sendall(b"+OK\r\n")
                elif cmd_name == "UNWATCH":
                    session["watched_keys"] = {}
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "WATCH":
                    for k in cmd_parts[1:]:
                        session["watched_keys"][k] = store.KEY_VERSIONS.get(k, 0)
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "EXEC":
                    if not session["is_transaction_active"]:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    
                    is_dirty = any(store.KEY_VERSIONS.get(k, 0) > v for k, v in session["watched_keys"].items())
                    
                    if is_dirty:
                        connection.sendall(b"*-1\r\n")
                    else:
                        responses = []
                        for q_cmd in session["transaction_queue"]:
                            class ResponseProxy:
                                def __init__(self): self.buf = b""
                                def sendall(self, d): self.buf += d
                            
                            proxy = ResponseProxy()
                            execute_command(q_cmd, proxy, session)
                            responses.append(proxy.buf)
                        
                        header = f"*{len(responses)}\r\n".encode()
                        connection.sendall(header + b"".join(responses))

                    session["is_transaction_active"] = False
                    session["transaction_queue"] = []
                    session["watched_keys"] = {}
                else:
                    execute_command(cmd_parts, connection, session)

    except Exception:
        pass
    finally:
        if session["subscribed_channels"]:
            with store.BLOCK_LOCK:
                for chan in session["subscribed_channels"]:
                    if chan in store.SUBSCRIBERS:
                        store.SUBSCRIBERS[chan].discard(connection)
        connection.close()

def main():
    # 1. Early AOF setup
    setup_aof()

    # 2. Argument parsing for persistence settings
    keys = ["--dir", "--dbfilename", "--appendonly", "--appenddirname", "--appendfilename", "--appendfsync"]
    for k in keys:
        if k in sys.argv:
            try:
                store.CONFIG[k[2:]] = sys.argv[sys.argv.index(k) + 1]
            except (ValueError, IndexError):
                pass

    # 3. Load data from RDB snapshot
    load_rdb()
    
    # 4. Initialize and replay AOF if enabled
    init_aof()
    replay_aof()

    # 5. Network setup
    port = 6379
    if "--port" in sys.argv:
        try:
            port = int(sys.argv[sys.argv.index("--port") + 1])
        except (ValueError, IndexError):
            pass
    
    # 6. Replication handshake
    if "--replicaof" in sys.argv:
        store.ROLE = "slave"
        idx = sys.argv.index("--replicaof")
        try:
            parts = sys.argv[idx + 1].split()
            if len(parts) == 2:
                m_host, m_port = parts[0], int(parts[1])
            else:
                m_host = sys.argv[idx + 1]
                m_port = int(sys.argv[idx + 2])
            threading.Thread(target=initiate_handshake, args=(m_host, m_port, port), daemon=True).start()
        except (ValueError, IndexError):
            pass

    # 7. Start server
    server = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Redis server started on port {port}")
    
    while True:
        client_sock, _ = server.accept()
        threading.Thread(target=handle_client, args=(client_sock,), daemon=True).start()

if __name__ == "__main__":
    main()
