import socket
import threading
import sys

# Import modul buatan kita sendiri
from app.store import KEY_VERSIONS
from app.protocol import parse_resp
from app.handlers import execute_command

def handle_client(connection):
    """Sang Konduktor: Mengatur aliran pesan dan transaksi per klien"""
    try:
        is_transaction_active = False
        transaction_queue = []
        watched_keys = {}

        while True:
            raw_data = connection.recv(4096)
            if not raw_data: break

            all_cmds = parse_resp(raw_data)
            for p in all_cmds:
                if not p: continue
                
                cmd_name = p[0].upper()

                # --- VALIDASI TRANSAKSI ---
                if is_transaction_active and cmd_name not in ["EXEC", "DISCARD"]:
                    if cmd_name in ["WATCH", "UNWATCH"]:
                        connection.sendall(f"-ERR {cmd_name} inside MULTI is not allowed\r\n".encode())
                    else:
                        transaction_queue.append(p)
                        connection.sendall(b"+QUEUED\r\n")
                    continue

                # --- LOGIKA MULTI / EXEC / DISCARD / WATCH ---
                if cmd_name == "MULTI":
                    is_transaction_active = True
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "DISCARD":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        is_transaction_active = False
                        transaction_queue = []
                        watched_keys = {}
                        connection.sendall(b"+OK\r\n")
                elif cmd_name == "UNWATCH":
                    watched_keys = {}
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "WATCH":
                    for k in p[1:]:
                        watched_keys[k] = KEY_VERSIONS.get(k, 0)
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "EXEC":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    
                    # Cek apakah ada kunci yang berubah (Optimistic Locking)
                    is_dirty = any(KEY_VERSIONS.get(k, 0) > v for k, v in watched_keys.items())
                    
                    if is_dirty:
                        connection.sendall(b"*-1\r\n")
                    else:
                        # Jalankan semua antrean menggunakan Proxy
                        res_list = []
                        for q_cmd in transaction_queue:
                            class Proxy:
                                def __init__(self): self.buf = b""
                                def sendall(self, d): self.buf += d
                            
                            prx = Proxy()
                            execute_command(q_cmd, prx)
                            res_list.append(prx.buf)
                        
                        # Kirim hasil kolektif
                        pk = f"*{len(res_list)}\r\n".encode()
                        for r in res_list: pk += r
                        connection.sendall(pk)

                    # Reset state setelah EXEC
                    is_transaction_active = False
                    transaction_queue = []
                    watched_keys = {}
                else:
                    # JALANKAN PERINTAH NORMAL
                    execute_command(p, connection)

    except Exception: pass
    finally: connection.close()

def main():
    port = 6379
    if "--port" in sys.argv:
        try: port = int(sys.argv[sys.argv.index("--port") + 1])
        except: pass

    # Buat server yang mampu menangani banyak thread
    server = socket.create_server(("localhost", port), reuse_port=True)
    while True:
        client_sock, _ = server.accept()
        threading.Thread(target=handle_client, args=(client_sock,)).start()

if __name__ == "__main__":
    main()
