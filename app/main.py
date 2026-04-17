import socket    # Alat untuk membuat koneksi jaringan (seperti kabel internet di dalam kode)
import threading  # Alat untuk melayani banyak klien sekaligus (multitasking)
import time       # Alat untuk melihat jam/waktu sekarang (dipakai untuk fitur kedaluwarsa)

# Ini adalah "Gudang Data" kita. Bentuknya Dictionary (kamus).
# Isinya: { "kunci": (nilai, waktu_basi) }
DATA_STORE = {}

# === Class khusus untuk membedakan Stream dengan List/String biasa ===
class Stream:
    def __init__(self):
        # Isinya adalah daftar (list) yang menampung tuple: (id_entri, dictionary_data)
        # Contoh: [ ("0-1", {"suhu": "36", "lembab": "95"}), ... ]
        self.entries = []


# === Peralatan baru untuk BLPOP (Blocking) ===
# BLOCK_LOCK = Kunci gembok. Sebelum mengecek/mengubah antrean tunggu, kita harus gembok dulu
# supaya dua thread tidak saling tabrakan.
BLOCK_LOCK = threading.Lock()

# BLOCKING_CLIENTS = Antrean orang yang sedang MENUNGGU barang di sebuah daftar.
# Bentuknya: { "nama_daftar": [(bel_1, kotak_hasil_1), (bel_2, kotak_hasil_2), ...] }
# - "bel" (Event) = alat untuk memberi tahu klien bahwa barangnya sudah datang
# - "kotak_hasil" (list) = tempat meletakkan barang yang sudah diambil
BLOCKING_CLIENTS = {}


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
            # PERINTAH: TYPE → Mengecek jenis data (string, list, dll) dari sebuah kunci
            # ─────────────────────────────────────────
            elif command == "TYPE":
                # TYPE <kunci>
                key = parts[4]

                # Cek apakah kunci ada di gudang
                if key in DATA_STORE:
                    data, expiry_time = DATA_STORE[key]

                    # Cek dulu apakah datanya sudah kedaluwarsa
                    if expiry_time is not None and time.time() > expiry_time:
                        del DATA_STORE[key]
                        response = "+none\r\n"
                    else:
                        # Cek jenis datanya pakai isinstance()
                        if isinstance(data, str):
                            response = "+string\r\n"
                        elif isinstance(data, list):
                            response = "+list\r\n"
                        elif isinstance(data, Stream):
                            response = "+stream\r\n"
                        else:
                            response = "+none\r\n"
                else:
                    # Kunci tidak ditemukan
                    response = "+none\r\n"

                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: XADD → Memasukkan entri data baru ke dalam Stream
            # ─────────────────────────────────────────
            elif command == "XADD":
                # XADD <kunci> <id> <field1> <value1> <field2> <value2> ...
                key = parts[4]      # Nama daftar Stream
                entry_id = parts[6] # ID unik (misal: "0-1")

                # Kumpulkan sisa argumen sebagai pasangan (field: value)
                fields = {}
                # Mulai dari index 8, lompati setiap 4 langkah 
                # (karena index antar data dipisah oleh format RESP panjang string)
                for i in range(8, len(parts) - 1, 4):
                    field = parts[i]
                    value = parts[i+2]
                    fields[field] = value

                # Cek apakah stream dengan kunci ini sudah ada
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]
                    if isinstance(data_lama, Stream):
                        # Kalau sudah ada, tambahkan entri baru ke dalamnya
                        data_lama.entries.append((entry_id, fields))
                    else:
                        # (Abaikan kalau tipenya salah untuk sementara)
                        pass
                else:
                    # Kalau belum ada, buat Stream baru
                    stream_baru = Stream()
                    stream_baru.entries.append((entry_id, fields))
                    DATA_STORE[key] = (stream_baru, None)

                # Balas dengan Bulk String berisi ID yang barusan ditambahkan
                response = f"${len(entry_id)}\r\n{entry_id}\r\n"
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
                # Kita simpan jumlahnya sekarang, SEBELUM membagikan ke klien yang menunggu
                response = f":{jumlah}\r\n"

                # === BARU: Setelah memasukkan barang, cek apakah ada orang yang menunggu ===
                # Gembok dulu supaya aman (tidak ada thread lain yang ikut mengubah antrean)
                with BLOCK_LOCK:
                    # Selama masih ada orang yang menunggu DAN daftar masih ada isinya...
                    while key in BLOCKING_CLIENTS and BLOCKING_CLIENTS[key]:
                        # Ambil data terbaru dari gudang
                        data, exp = DATA_STORE[key]
                        if isinstance(data, list) and len(data) > 0:
                            # Ambil elemen pertama dari daftar untuk diberikan ke orang yang menunggu
                            elemen = data.pop(0)
                            DATA_STORE[key] = (data, exp)

                            # Ambil orang pertama di antrean tunggu (yang paling lama menunggu)
                            bel, kotak_hasil = BLOCKING_CLIENTS[key].pop(0)
                            # Taruh barangnya di kotak hasil milik orang itu
                            kotak_hasil.append(elemen)
                            # Bunyikan bel! "Barangmu sudah datang!"
                            bel.set()
                        else:
                            # Daftar sudah habis, berhenti membagikan
                            break

                    # Bersihkan antrean kosong
                    if key in BLOCKING_CLIENTS and not BLOCKING_CLIENTS[key]:
                        del BLOCKING_CLIENTS[key]

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
                        if start < 0:
                            start = panjang + start
                        if stop < 0:
                            stop = panjang + stop

                        # Aturan Redis: kalau index masih negatif, jadikan 0
                        if start < 0:
                            start = 0
                        if stop < 0:
                            stop = 0

                        # Potong daftarnya dari start sampai stop (+1 karena aturan Python)
                        potongan = data_lama[start:stop + 1]

                        # Susun jawaban dengan format Array RESP
                        response = f"*{len(potongan)}\r\n"
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
            # PERINTAH: LPOP → Ambil dan hapus elemen dari DEPAN daftar
            # ─────────────────────────────────────────
            elif command == "LPOP":
                # LPOP <kunci> [jumlah]
                key = parts[4]  # Nama daftar yang mau diambil elemennya

                # Cek apakah ada argumen jumlah (opsional)
                count = None
                if len(parts) > 6 and parts[5].startswith("$"):
                    count = int(parts[6])  # Berapa banyak elemen yang mau diambil

                # Cek apakah kunci ada di gudang
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]  # Ambil isinya

                    # Pastikan isinya adalah list dan tidak kosong
                    if isinstance(data_lama, list) and len(data_lama) > 0:

                        if count is not None:
                            # === Mode BANYAK: ambil beberapa elemen dari depan ===
                            diambil = data_lama[:count]
                            sisa = data_lama[count:]
                            DATA_STORE[key] = (sisa, expiry)

                            # Susun jawaban sebagai Array RESP
                            response = f"*{len(diambil)}\r\n"
                            for item in diambil:
                                response += f"${len(item)}\r\n{item}\r\n"
                        else:
                            # === Mode SATU: ambil 1 elemen pertama saja ===
                            elemen_pertama = data_lama.pop(0)
                            DATA_STORE[key] = (data_lama, expiry)

                            # Balas dengan elemen tunggal dalam format Bulk String
                            response = f"${len(elemen_pertama)}\r\n{elemen_pertama}\r\n"
                    else:
                        # Daftar kosong atau bukan list, balas dengan null
                        response = "$-1\r\n"
                else:
                    # Kunci tidak ditemukan di gudang, balas dengan null
                    response = "$-1\r\n"

                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: BLPOP → MENUNGGU lalu ambil elemen dari DEPAN daftar
            # Ini seperti LPOP, tapi kalau daftarnya kosong, klien akan MENUNGGU
            # sampai ada orang lain yang memasukkan barang ke daftar itu.
            # ─────────────────────────────────────────
            elif command == "BLPOP":
                # BLPOP <kunci> <timeout_detik>
                key = parts[4]           # Nama daftar yang mau ditunggu
                timeout = float(parts[6])  # Pakai float supaya bisa terima 0.1 atau 0.5 detik

                sudah_dapat = False  # Tanda apakah kita sudah dapat barangnya

                # Gembok dulu sebelum mengecek gudang dan antrean tunggu
                with BLOCK_LOCK:
                    # Cek apakah daftar sudah punya isi
                    if key in DATA_STORE:
                        data, exp = DATA_STORE[key]
                        if isinstance(data, list) and len(data) > 0:
                            # Ada barang! Langsung ambil yang pertama
                            elemen = data.pop(0)
                            DATA_STORE[key] = (data, exp)
                            sudah_dapat = True

                if sudah_dapat:
                    # Balas langsung dengan Array berisi [nama_daftar, elemen_yang_diambil]
                    response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(elemen)}\r\n{elemen}\r\n"
                    connection.sendall(response.encode())
                else:
                    # Daftar kosong! Kita harus MENUNGGU...
                    # Siapkan "bel" dan "kotak hasil" untuk klien ini
                    bel = threading.Event()    # Bel yang akan dibunyikan saat barang datang
                    kotak_hasil = []           # Kotak kosong untuk menerima barang nanti
                    client_info = (bel, kotak_hasil)

                    # Daftarkan diri ke antrean tunggu (dengan gembok supaya aman)
                    with BLOCK_LOCK:
                        if key not in BLOCKING_CLIENTS:
                            BLOCKING_CLIENTS[key] = []  # Buat antrean baru kalau belum ada
                        BLOCKING_CLIENTS[key].append(client_info)  # Masuk antrean

                    # === MENUNGGU DI SINI ===
                    # Thread ini akan "tidur" sampai bel dibunyikan oleh RPUSH
                    if timeout == 0:
                        bel.wait()  # Tunggu selamanya
                    else:
                        bel.wait(timeout=timeout)  # Tunggu maksimal sekian detik (float)

                    # Bel sudah berbunyi ATAU waktu habis. 
                    # Jika waktu habis, kita harus hapus diri kita dari antrean supaya tidak dikasih barang lagi nanti
                    with BLOCK_LOCK:
                        if not kotak_hasil:
                            # Kalau kotak masih kosong, berarti kita berhenti karena waktu habis
                            if key in BLOCKING_CLIENTS and client_info in BLOCKING_CLIENTS[key]:
                                BLOCKING_CLIENTS[key].remove(client_info)

                    # Cek hasil akhir
                    if kotak_hasil:
                        elemen = kotak_hasil[0]  # Ambil barang dari kotak
                        # Balas dengan Array berisi [nama_daftar, elemen]
                        response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(elemen)}\r\n{elemen}\r\n"
                        connection.sendall(response.encode())
                    else:
                        # Waktu habis benar-benar tidak ada barang. Balas null array.
                        response = "*-1\r\n"
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
