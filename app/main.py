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
        # State (status) per koneksi klien
        is_transaction_active = False
        transaction_queue = []
        watched_keys = {}

        while True:
            raw_data = connection.recv(4096)
            if not raw_data: break

            all_cmds = parse_resp(raw_data)
            for p, _ in all_cmds:
                if not p: continue
                
                cmd_name = p[0].upper()

                # --- PENANGANAN TRANSAKSI (QUEUEING) ---
                if is_transaction_active and cmd_name not in ["EXEC", "DISCARD"]:
                    # Perintah WATCH/UNWATCH dilarang saat MULTI sedang aktif
                    if cmd_name in ["WATCH", "UNWATCH"]:
                        connection.sendall(f"-ERR {cmd_name} inside MULTI is not allowed\r\n".encode())
                    else:
                        # Masukkan ke antrean dan beri tahu klien
                        transaction_queue.append(p)
                        connection.sendall(b"+QUEUED\r\n")
                    continue

                # --- LOGIKA KONTROL (MULTI, EXEC, DISCARD, WATCH) ---
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
                    # Catat versi kunci untuk Optimistic Locking
                    for k in p[1:]:
                        watched_keys[k] = store.KEY_VERSIONS.get(k, 0)
                    connection.sendall(b"+OK\r\n")
                elif cmd_name == "EXEC":
                    if not is_transaction_active:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                        continue
                    
                    # VALIDASI WATCH: Apakah ada kunci yang berubah sejak di-WATCH?
                    is_dirty = any(store.KEY_VERSIONS.get(k, 0) > v for k, v in watched_keys.items())
                    
                    if is_dirty:
                        # Batalkan transaksi jika ada konflik
                        connection.sendall(b"*-1\r\n")
                    else:
                        # Jalankan semua antrean menggunakan Proxy agar output bisa dikumpulkan
                        res_list = []
                        for q_cmd in transaction_queue:
                            class Proxy:
                                def __init__(self): self.buf = b""
                                def sendall(self, d): self.buf += d
                            
                            prx = Proxy()
                            execute_command(q_cmd, prx)
                            res_list.append(prx.buf)
                        
                        # Kirim semua balasan sebagai Array RESP
                        pk = f"*{len(res_list)}\r\n".encode()
                        for r in res_list: pk += r
                        connection.sendall(pk)

                    # Reset status transaksi setelah EXEC selesai
                    is_transaction_active = False
                    transaction_queue = []
                    watched_keys = {}
                else:
                    # JALANKAN PERINTAH SECARA NORMAL (IMMEDIATE MODE)
                    execute_command(p, connection)

    except Exception: pass
    finally: connection.close()

import app.store as store

def initiate_handshake(master_host, master_port, my_port):
    """Fungsi khusus Slave untuk Handshake (PING & REPLCONF)"""
    try:
        master_conn = socket.create_connection((master_host, master_port))
        
        # 1. Kirim PING
        master_conn.sendall(b"*1\r\n$4\r\nPING\r\n")
        master_conn.recv(1024) # Tunggu balasan PONG
        
        # 2. Kirim REPLCONF listening-port
        cmd1 = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(my_port))}\r\n{my_port}\r\n"
        master_conn.sendall(cmd1.encode())
        master_conn.recv(1024) # Tunggu balasan OK
        
        # 3. Kirim REPLCONF capa psync2
        cmd2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_conn.sendall(cmd2.encode())
        master_conn.recv(1024) # Tunggu balasan OK
        
        # 4. Kirim PSYNC ? -1
        cmd3 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_conn.sendall(cmd3.encode())
        
        # 5. Lewati balasan FULLRESYNC dan file RDB kosong
        # Kita baca dulu balasannya (+FULLRESYNC...)
        resp = master_conn.recv(1024)
        
        # Jika balasan berisi RDB ($...), kita pastikan sudah terbaca semua
        # Di tahap ini RDB sangat kecil, jadi recv(1024) biasanya sudah cukup mengambil semuanya
        
        # 6. MODE PROPAGASI: Tetap terhubung dan dengerin perintah dari Master
        while True:
            data = master_conn.recv(4096)
            if not data: break
            
            # Terjemahkan perintah dari Master dan hitung byte-nya
            all_cmds = parse_resp(data)
            for c_cmd, cmd_len in all_cmds:
                if not c_cmd: continue
                
                # Jalankan perintah: 
                # Slave tetap diam untuk SET/PING, tapi WAJIB jawab untuk REPLCONF ACK
                class ReplicationProxy:
                    def sendall(self, d):
                        if b"REPLCONF" in d and b"ACK" in d:
                            master_conn.sendall(d)
                
                execute_command(c_cmd, ReplicationProxy())
                # UPDATE OFFSET: Tambahkan panjang byte perintah ini ke total offset
                store.REPLICA_OFFSET += cmd_len

    except Exception as e:
        print(f"Gagal jabat tangan atau koneksi Master terputus: {e}")

def main():
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
