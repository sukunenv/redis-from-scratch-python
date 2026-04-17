import socket    # Alat untuk membuat koneksi jaringan (seperti kabel internet di dalam kode)
import threading  # Alat untuk melayani banyak klien sekaligus (multitasking)
import time       # Alat untuk melihat jam/waktu sekarang (dipakai untuk fitur kedaluwarsa)

# Ini adalah "Gudang Data" kita. Bentuknya Dictionary (kamus).
# Isinya: { "kunci": (nilai, waktu_basi) }
DATA_STORE = {}


def handle_client(connection):
    # Fungsi ini dijalankan untuk setiap klien yang terhubung
    try:
        while True:
            # Mendengarkan dan menerima pesan dari klien (maksimal 1024 karakter)
            data = connection.recv(1024)

            # Jika klien memutus koneksi, data akan kosong, kita berhenti
            if not data:
                break

            # Mengubah pesan (bytes) jadi teks biasa, lalu dipotong berdasarkan Enter (\r\n)
            # Hasilnya misal: ['*2', '$4', 'PING', '']
            parts = data.decode().split("\r\n")

            # Perintahnya selalu ada di bagian ke-3 (index ke-2), kita ubah jadi huruf besar
            command = parts[2].upper()

            # ─────────────────────────────────────────
            # PERINTAH: PING → Balas PONG (Sekedar sapa)
            # ─────────────────────────────────────────
            if command == "PING":
                connection.sendall(b"+PONG\r\n")

            # ─────────────────────────────────────────
            # PERINTAH: ECHO → Memantulkan balik kata yang dikirim
            # ─────────────────────────────────────────
            elif command == "ECHO":
                # ECHO <pesan> — pesan ada di bagian ke-5 (index ke-4)
                payload = parts[4]
                # Susun balasan format: $panjang\r\nisi\r\n
                response = f"${len(payload)}\r\n{payload}\r\n"
                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: SET → Menyimpan data ke gudang
            # ─────────────────────────────────────────
            elif command == "SET":
                # SET <kunci> <nilai> [PX <milidetik>]
                key = parts[4]    # Nama kunci (nama laci)
                value = parts[6]  # Isi nilainya (isi laci)

                # Defaultnya data tidak punya waktu kedaluwarsa
                expiry_time = None

                # Cek apakah ada kata "PX" (tanda bahwa ada batas waktu)
                if len(parts) > 8 and parts[8].upper() == "PX":
                    px_value = int(parts[10])  # Ambil angka milidetiknya
                    # Hitung waktu kedaluwarsa: jam sekarang + batas waktu (diubah ke detik)
                    expiry_time = time.time() + (px_value / 1000.0)

                # Simpan ke gudang dalam bentuk pasangan: (isi, waktu_basi)
                DATA_STORE[key] = (value, expiry_time)

                # Balas klien dengan tanda sukses
                connection.sendall(b"+OK\r\n")

            # ─────────────────────────────────────────
            # PERINTAH: GET → Mengambil data dari gudang
            # ─────────────────────────────────────────
            elif command == "GET":
                # GET <kunci>
                key = parts[4]  # Nama kunci yang mau diambil

                # Cek apakah kunci ini ada di gudang
                if key in DATA_STORE:
                    value, expiry_time = DATA_STORE[key]  # Ambil isi dan waktu basinya

                    # Cek: apakah sudah melewati waktu kedaluwarsa?
                    if expiry_time is not None and time.time() > expiry_time:
                        del DATA_STORE[key]   # Hapus data yang sudah basi dari gudang
                        response = "$-1\r\n"  # Beritahu klien: "Datanya sudah tidak ada"
                    else:
                        # Data masih segar, kirimkan isinya
                        response = f"${len(value)}\r\n{value}\r\n"
                else:
                    # Kunci tidak ditemukan sama sekali di gudang
                    response = "$-1\r\n"

                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: RPUSH → Memasukkan elemen ke BELAKANG daftar
            # ─────────────────────────────────────────
            elif command == "RPUSH":
                # RPUSH <kunci> <elemen1> <elemen2> ...
                key = parts[4]  # Nama daftar yang mau diisi

                # Tampung semua elemen baru yang dikirim ke dalam list sementara
                elemen_baru = []
                # Mulai dari index 6, lompat 2 langkah (karena ada $panjang di antara setiap elemen)
                for i in range(6, len(parts) - 1, 2):
                    elemen_baru.append(parts[i])

                # Cek apakah daftar dengan kunci ini sudah pernah dibuat sebelumnya
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil daftar lamanya

                    if isinstance(data_lama, list):
                        # Daftar sudah ada, tambahkan elemen baru ke belakang daftar lama
                        data_lama.extend(elemen_baru)
                        jumlah = len(data_lama)  # Hitung total isinya sekarang
                    else:
                        # Kunci ada tapi isinya bukan list (dari SET), ganti jadi list baru
                        DATA_STORE[key] = (elemen_baru, None)
                        jumlah = len(elemen_baru)
                else:
                    # Kunci belum ada, buat daftar baru dari nol
                    DATA_STORE[key] = (elemen_baru, None)
                    jumlah = len(elemen_baru)

                # Balas dengan total jumlah elemen dalam format Integer (:jumlah\r\n)
                response = f":{jumlah}\r\n"
                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: LPUSH → Memasukkan elemen dari DEPAN daftar
            # ─────────────────────────────────────────
            elif command == "LPUSH":
                # LPUSH <kunci> <elemen1> <elemen2> ...
                # Catatan: LPUSH "a" "b" "c" → hasilnya ["c", "b", "a"] (terbalik dari urutan input)
                key = parts[4]  # Nama daftar yang mau diisi dari depan

                # Tampung semua elemen baru yang dikirim ke dalam list sementara
                elemen_baru = []
                for i in range(6, len(parts) - 1, 2):
                    elemen_baru.append(parts[i])

                # Balik urutan elemen baru, lalu tempelkan ke DEPAN daftar lama
                elemen_baru_terbalik = list(reversed(elemen_baru))

                # Cek apakah daftar dengan kunci ini sudah pernah dibuat sebelumnya
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil daftar lamanya

                    if isinstance(data_lama, list):
                        # Gabungkan elemen baru (terbalik) + daftar lama = daftar lengkap baru
                        daftar_baru = elemen_baru_terbalik + data_lama
                        DATA_STORE[key] = (daftar_baru, expiry)  # Simpan kembali ke gudang
                        jumlah = len(daftar_baru)
                    else:
                        # Kunci ada tapi isinya bukan list, ganti jadi list baru
                        DATA_STORE[key] = (elemen_baru_terbalik, None)
                        jumlah = len(elemen_baru_terbalik)
                else:
                    # Kunci belum ada, buat daftar baru dari nol
                    DATA_STORE[key] = (elemen_baru_terbalik, None)
                    jumlah = len(elemen_baru_terbalik)

                # Balas dengan total jumlah elemen dalam format Integer (:jumlah\r\n)
                response = f":{jumlah}\r\n"
                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: LRANGE → Melihat isi daftar (sebagian atau seluruhnya)
            # ─────────────────────────────────────────
            elif command == "LRANGE":
                # LRANGE <kunci> <mulai> <berhenti>
                key = parts[4]         # Nama daftar yang mau dilihat
                start = int(parts[6])  # Urutan awal yang mau diambil
                stop = int(parts[8])   # Urutan akhir yang mau diambil

                # Cek apakah kunci ini ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil isi daftar

                    # Pastikan isinya adalah sebuah list (daftar)
                    if isinstance(data_lama, list):
                        panjang = len(data_lama)  # Hitung panjang daftar

                        # Ubah index negatif menjadi positif (dihitung dari belakang)
                        # Contoh: -1 pada daftar 5 elemen → 5 + (-1) = 4 (index terakhir)
                        if start < 0:
                            start = panjang + start
                        if stop < 0:
                            stop = panjang + stop

                        # Aturan Redis: kalau index masih negatif (terlalu jauh mundur),
                        # anggap saja mulai dari 0 (paling depan)
                        if start < 0:
                            start = 0
                        if stop < 0:
                            stop = 0

                        # Potong daftarnya dari start sampai stop
                        # (+1 karena Python tidak ikutkan angka terakhir dalam slice)
                        potongan = data_lama[start:stop + 1]

                        # Susun jawaban dengan format Array RESP, awali dengan jumlah elemen
                        response = f"*{len(potongan)}\r\n"

                        # Tambahkan setiap elemen satu per satu ke dalam jawaban
                        for item in potongan:
                            response += f"${len(item)}\r\n{item}\r\n"
                    else:
                        # Data ada tapi bukan daftar, balas dengan array kosong
                        response = "*0\r\n"
                else:
                    # Kunci tidak ditemukan, balas dengan array kosong
                    response = "*0\r\n"

                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: LLEN → Menghitung jumlah elemen dalam daftar
            # ─────────────────────────────────────────
            elif command == "LLEN":
                # LLEN <kunci>
                key = parts[4]  # Nama daftar yang mau dihitung panjangnya

                # Cek apakah kunci ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil isinya

                    if isinstance(data_lama, list):
                        # Hitung jumlah elemen menggunakan len() (fungsi bawaan Python)
                        jumlah = len(data_lama)
                    else:
                        # Kunci ada tapi bukan list, anggap panjangnya 0
                        jumlah = 0
                else:
                    # Kunci tidak ditemukan di gudang, kembalikan 0
                    jumlah = 0

                # Balas dengan jumlah dalam format Integer (:jumlah\r\n)
                response = f":{jumlah}\r\n"
                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: LPOP → Ambil dan hapus elemen PERTAMA dari daftar
            # ─────────────────────────────────────────
            elif command == "LPOP":
                # LPOP <kunci>
                key = parts[4]  # Nama daftar yang mau diambil elemen pertamanya

                # Cek apakah kunci ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil isinya

                    # Pastikan isinya adalah list dan tidak kosong
                    if isinstance(data_lama, list) and len(data_lama) > 0:
                        # .pop(0) = ambil sekaligus hapus elemen di posisi pertama (index 0)
                        elemen_pertama = data_lama.pop(0)

                        # Simpan kembali daftar yang sudah berkurang satu ke gudang
                        DATA_STORE[key] = (data_lama, expiry)

                        # Balas dengan elemen yang diambil dalam format Bulk String
                        response = f"${len(elemen_pertama)}\r\n{elemen_pertama}\r\n"
                    else:
                        # Daftar kosong atau bukan list, balas dengan null
                        response = "$-1\r\n"
                else:
                    # Kunci tidak ditemukan di gudang, balas dengan null
                    response = "$-1\r\n"

                connection.sendall(response.encode())


    except Exception:
        # Kalau ada error tak terduga, kita diamkan saja supaya server tidak mati
        pass
    finally:
        # Apapun yang terjadi, pastikan koneksi dengan klien ini ditutup dengan baik
        connection.close()


def main():
    # Membuat server di alamat localhost (komputer sendiri) port 6379 (port standar Redis)
    # reuse_port=True: supaya port bisa langsung dipakai lagi setelah server dimatikan
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Terus berjalan selamanya, menunggu klien yang mau konek
    while True:
        # Tunggu sampai ada klien yang menghubungi, lalu simpan koneksinya
        connection, _ = server_socket.accept()

        # Jalankan fungsi handle_client di "thread" (jalur kerja) baru
        # Supaya server bisa melayani klien berikutnya tanpa menunggu yang ini selesai
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.start()  # Mulai thread baru


if __name__ == "__main__":
    # Titik awal program. Hanya jalan kalau file ini dijalankan langsung (bukan diimport)
    main()
