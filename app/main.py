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
                
            elif command == "RPUSH":
                # RPUSH <kunci> <elemen1> <elemen2> ...
                key = parts[4]
                
                # Kita buat daftar kosong untuk menampung elemen-elemen baru
                elemen_baru = []
                # Mulai dari index 6, lompat 2 langkah setiap kali (karena ada $panjang di antaranya)
                for i in range(6, len(parts) - 1, 2):
                    elemen_baru.append(parts[i])
                
                # Cek apakah kunci sudah ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]
                    if isinstance(data_lama, list):
                        # Tambahkan semua elemen baru ke daftar lama
                        data_lama.extend(elemen_baru)
                        jumlah = len(data_lama)
                    else:
                        DATA_STORE[key] = (elemen_baru, None)
                        jumlah = len(elemen_baru)
                else:
                    DATA_STORE[key] = (elemen_baru, None)
                    jumlah = len(elemen_baru)
                
                response = f":{jumlah}\r\n"
                connection.sendall(response.encode())
                
            elif command == "LRANGE":
                # LRANGE <kunci> <mulai> <berhenti>
                key = parts[4]
                start = int(parts[6])
                stop = int(parts[8])
                
                # Cek apakah kunci ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]
                    
                    # Pastikan ini adalah sebuah daftar (list)
                    if isinstance(data_lama, list):
                        panjang = len(data_lama)
                        
                        # Ubah index negatif jadi positif (menghitung dari belakang)
                        if start < 0:
                            start = panjang + start
                        if stop < 0:
                            stop = panjang + stop
                            
                        # Aturan Redis: kalau terlalu mundur ke belakang, anggap saja 0 (paling depan)
                        if start < 0:
                            start = 0
                        if stop < 0:
                            stop = 0
                            
                        # Ambil potongan dari start sampai stop (berhenti ditambah 1 karena aturan Python)
                        potongan = data_lama[start:stop + 1]
                        
                        # Mulai menyusun jawaban berformat Array (contoh: *3\r\n)
                        response = f"*{len(potongan)}\r\n"
                        
                        # Menggabungkan setiap elemen ke dalam jawaban (contoh: $1\r\na\r\n)
                        for item in potongan:
                            response += f"${len(item)}\r\n{item}\r\n"
                    else:
                        # Jika datanya ternyata bukan list, balas array kosong
                        response = "*0\r\n"
                else:
                    # Jika kuncinya tidak ada, balas array kosong
                    response = "*0\r\n"
                    
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
