import socket
import threading
import sys

# Mengambil alat-alat yang kita butuhkan dari modul lain di dalam folder app
from app.protocol import parse_resp
from app.handlers import execute_command
import app.store as store

def handle_client(connection):
    """
    SANG KONDUKTOR: Fungsi ini melayani setiap klien yang terhubung.
    Di sini kita mengatur apakah perintah dijalankan langsung atau masuk antrean MULTI.
    """
    try:
        # State (status) per koneksi klien dalam satu "Sesi"
        session = {
            "is_transaction_active": False,
            "transaction_queue": [],
            "watched_keys": {},
            "subscribed_channels": set()
        }

        while True:
            raw_data = connection.recv(4096)
            if not raw_data: break

            all_cmds = parse_resp(raw_data)
            for p, _ in all_cmds:
                if not p: continue
                
                cmd_name = p[0].upper()

                # --- PENANGANAN TRANSAKSI (QUEUEING) ---
                if session["is_transaction_active"] and cmd_name not in ["EXEC", "DISCARD"]:
                    # Perintah WATCH/UNWATCH dilarang saat MULTI sedang aktif
                    if cmd_name in ["WATCH", "UNWATCH"]:
                        connection.sendall(f"-ERR {cmd_name} inside MULTI is not allowed\r\n".encode())
                    else:
                        # Masukkan ke antrean dan beri tahu klien
                        session["transaction_queue"].append(p)
                        connection.sendall(b"+QUEUED\r\n")
                    continue

                # --- FILTER SUBSCRIBED MODE (Satpam Mode) ---
                # Jika klien sedang langganan, perintah yang boleh lewat terbatas
                if len(session["subscribed_channels"]) > 0:
                    allowed = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"]
                    if cmd_name not in allowed:
                        connection.sendall(f"-ERR Can't execute '{cmd_name.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode())
                        continue

                # --- LOGIKA KONTROL (MULTI, EXEC, DISCARD, WATCH) ---
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
                    # Catat versi kunci untuk Optimistic Locking
                    for k in p[1:]:
                        session["watched_keys"][k] = store.KEY_VERSIONS.get(k, 0)
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "EXEC":
                    if not session["is_transaction_active"]:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    
                    # VALIDASI WATCH: Apakah ada kunci yang berubah sejak di-WATCH?
                    is_dirty = any(store.KEY_VERSIONS.get(k, 0) > v for k, v in session["watched_keys"].items())
                    
                    if is_dirty:
                        # Batalkan transaksi jika ada konflik
                        connection.sendall(b"*-1\r\n")
                    else:
                        # Jalankan semua antrean menggunakan Proxy agar output bisa dikumpulkan
                        res_list = []
                        for q_cmd in session["transaction_queue"]:
                            class Proxy:
                                def __init__(self): self.buf = b""
                                def sendall(self, d): self.buf += d
                            
                            prx = Proxy()
                            execute_command(q_cmd, prx, session)
                            res_list.append(prx.buf)
                        
                        # Kirim semua balasan sebagai Array RESP
                        pk = f"*{len(res_list)}\r\n".encode()
                        for r in res_list: pk += r
                        connection.sendall(pk)

                    # Reset status transaksi setelah EXEC selesai
                    session["is_transaction_active"] = False
                    session["transaction_queue"] = []
                    session["watched_keys"] = {}
                else:
                    # JALANKAN PERINTAH SECARA NORMAL (IMMEDIATE MODE)
                    execute_command(p, connection, session)

    except Exception: pass
    finally:
        # PEMBERSIHAN: Hapus klien dari daftar langganan global jika dia pergi
        if session["subscribed_channels"]:
            with store.BLOCK_LOCK:
                for chan in session["subscribed_channels"]:
                    if chan in store.SUBSCRIBERS:
                        store.SUBSCRIBERS[chan].discard(connection)
        connection.close()

import app.store as store

from app.replication import initiate_handshake
from app.rdb import load_rdb

def main():
    # Mendukung argumen konfigurasi RDB
    if "--dir" in sys.argv:
        try: store.CONFIG["dir"] = sys.argv[sys.argv.index("--dir") + 1]
        except: pass
    if "--dbfilename" in sys.argv:
        try: store.CONFIG["dbfilename"] = sys.argv[sys.argv.index("--dbfilename") + 1]
        except: pass

    # MUAT DATA DARI RDB: Baca database dari file jika ada
    load_rdb()

    # Mendukung argumen port (misal: --port 6380)
    port = 6379
    if "--port" in sys.argv:
        try: port = int(sys.argv[sys.argv.index("--port") + 1])
        except: pass
    
    # Mendukung argumen --replicaof <master_host> <master_port>
    if "--replicaof" in sys.argv:
        store.ROLE = "slave"
        idx = sys.argv.index("--replicaof")
        args = sys.argv[idx + 1].split()
        if len(args) == 2:
            m_host, m_port = args[0], int(args[1])
        else:
            m_host = sys.argv[idx + 1]
            m_port = int(sys.argv[idx + 2])
        
        # Mulai proses jabat tangan dengan membawa informasi port kita sendiri
        threading.Thread(target=initiate_handshake, args=(m_host, m_port, port)).start()

    # Buat server utama
    server = socket.create_server(("localhost", port), reuse_port=True)
    while True:
        client_sock, _ = server.accept()
        # Jalankan thread baru untuk setiap klien
        threading.Thread(target=handle_client, args=(client_sock,)).start()

if __name__ == "__main__":
    main()
