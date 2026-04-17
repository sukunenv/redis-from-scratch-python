import socket
import threading
import time

# Ini adalah "Gudang Data" kita (Dictionary)
DATA_STORE = {}


def handle_client(connection):
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            
            # Memecah pesan menjadi bagian-bagian
            parts = data.decode().split("\r\n")
            command = parts[2].upper()
            
            if command == "PING":
                connection.sendall(b"+PONG\r\n")
                
            elif command == "ECHO":
                # ECHO <pesan>
                payload = parts[4]
                response = f"${len(payload)}\r\n{payload}\r\n"
                connection.sendall(response.encode())
                
            elif command == "SET":
                # SET <kunci> <nilai> [PX <milidetik>]
                key = parts[4]
                value = parts[6]
                
                # Kita cek apakah pesan cukup panjang dan ada tambahan PX
                expiry_time = None
                if len(parts) > 8 and parts[8].upper() == "PX":
                    # Menambahkan waktu sekarang dengan batas milidetik (dibagi 1000 jadi detik)
                    px_value = int(parts[10])
                    expiry_time = time.time() + (px_value / 1000.0)
                    
                # Simpan berpasangan: (isi data, waktu basi)
                DATA_STORE[key] = (value, expiry_time)
                connection.sendall(b"+OK\r\n")
                
            elif command == "GET":
                # GET <kunci>
                key = parts[4]
                
                # Jika data ada di dalam gudang
                if key in DATA_STORE:
                    value, expiry_time = DATA_STORE[key]
                    
                    # Mengecek apakah sudah kedaluwarsa?
                    if expiry_time is not None and time.time() > expiry_time:
                        # Hapus data yang sudah basi
                        del DATA_STORE[key]
                        response = "$-1\r\n"
                    else:
                        # Jika masih segar, kirim datanya
                        response = f"${len(value)}\r\n{value}\r\n"
                else:
                    # Jika data memang tidak ada
                    response = "$-1\r\n"
                    
                connection.sendall(response.encode())
                
    except Exception:
        pass
    finally:
        connection.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.start()


if __name__ == "__main__":
    main()
