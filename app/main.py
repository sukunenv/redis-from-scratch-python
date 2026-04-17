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

# STREAM_BLOCKING_CLIENTS = Antrean orang yang menunggu data baru di Stream.
# Berbeda dengan List, Stream tidak "menghilangkan" data saat dibaca,
# jadi kita cukup menyimpan bel untuk membangunkan orang-orang ini saat ada XADD baru.
STREAM_BLOCKING_CLIENTS = {}

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
            # PERINTAH: INCR → Menambahkan nilai angka sebanyak 1
            # ─────────────────────────────────────────
            elif command == "INCR":
                # INCR <kunci>
                key = parts[4]

                # Tahap ini hanya fokus pada kasus di mana kunci sudah ada.
                if key in DATA_STORE:
                    value, expiry_time = DATA_STORE[key]
                    
                    try:
                        # Coba ubah teks jadi angka
                        angka = int(value)
                        angka += 1
                        
                        # Simpan kembali ke gudang
                        DATA_STORE[key] = (str(angka), expiry_time)
                        
                        # Balas klien dengan angka barunya
                        response = f":{angka}\r\n"
                    except ValueError:
                        # Jika gagal (isinya bukan angka), balas dengan error sesuai standar Redis
                        response = "-ERR value is not an integer or out of range\r\n"
                else:
                    # Jika kunci belum ada, buat baru dengan nilai 1
                    DATA_STORE[key] = ("1", None)
                    response = ":1\r\n" 
                
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
                entry_id = parts[6] # ID unik (misal: "0-1", "123-*", atau "*")

                # ========================================================
                # FITUR AUTO-GENERATE FULL (*) ATAU SEBAGIAN (123-*)
                # ========================================================
                if entry_id == "*":
                    # Klien minta buatkan ID sepenuhnya. Ambil waktu server saat ini (milidetik).
                    ms_time = int(time.time() * 1000)
                    
                    if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and len(DATA_STORE[key][0].entries) > 0:
                        last_id = DATA_STORE[key][0].entries[-1][0]
                        last_ms, last_seq = map(int, last_id.split("-"))
                        
                        if ms_time == last_ms:
                            # Waktu persis sama dengan data terakhir
                            seq_num = last_seq + 1
                        elif ms_time < last_ms:
                            # (Jaga-jaga kalau waktu server mundur karena sinkronisasi jam)
                            ms_time = last_ms
                            seq_num = last_seq + 1
                        else:
                            seq_num = 0
                    else:
                        seq_num = 0
                        
                    entry_id = f"{ms_time}-{seq_num}"

                else:
                    # Pecah ID sementara jadi 2 bagian teks: waktu(ms) dan nomor_urut
                    ms_time_str, seq_num_str = entry_id.split("-")
                    ms_time = int(ms_time_str)

                    if seq_num_str == "*":
                        if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream) and len(DATA_STORE[key][0].entries) > 0:
                            # Ada data sebelumnya, mari kita intip ID terakhir
                            last_id = DATA_STORE[key][0].entries[-1][0]
                            last_ms, last_seq = map(int, last_id.split("-"))

                            if ms_time == last_ms:
                                # Kalau waktunya sama dengan yang terakhir, nomor urut ditambah 1
                                seq_num = last_seq + 1
                            else:
                                # Kalau waktunya baru, mulai dari 0 (kecuali waktunya 0, mulai dari 1)
                                seq_num = 0 if ms_time > 0 else 1
                        else:
                            # Daftar masih kosong sama sekali, mulai dari 0 (kecuali waktunya 0, mulai dari 1)
                            seq_num = 0 if ms_time > 0 else 1
                        
                        # Rangkai kembali menjadi ID utuh
                        entry_id = f"{ms_time}-{seq_num}"
                    else:
                        # Kalau bukan bintang, ubah saja jadi angka biasa
                        seq_num = int(seq_num_str)

                # ========================================================
                # ATURAN VALIDASI KETAT
                # ========================================================
                # Aturan 1: ID 0-0 dilarang keras!
                if entry_id == "0-0":
                    connection.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    continue # Langsung lanjut ke perintah klien berikutnya

                # Ambil sisa argumen sebagai pasangan (field: value)
                fields = {}
                for i in range(8, len(parts) - 1, 4):
                    fields[parts[i]] = parts[i+2]

                # Siapkan variabel untuk ngecek apakah ID valid
                is_valid = True

                # Cek apakah stream dengan kunci ini sudah ada
                if key in DATA_STORE:
                    data_lama, expiry = DATA_STORE[key]
                    if isinstance(data_lama, Stream):
                        # Aturan 2: ID baru harus LEBIH BESAR dari ID terakhir
                        if len(data_lama.entries) > 0:
                            last_id = data_lama.entries[-1][0] # Ambil ID paling ujung belakang
                            last_ms, last_seq = map(int, last_id.split("-"))

                            if ms_time < last_ms:
                                is_valid = False
                            elif ms_time == last_ms and seq_num <= last_seq:
                                is_valid = False


                        if is_valid:
                            # Kalau valid, tambahkan entri baru
                            data_lama.entries.append((entry_id, fields))
                    else:
                        pass # Abaikan kalau tipenya bukan stream
                else:
                    # Kalau belum ada, buat Stream baru (ID pasti valid karena ini yang pertama)
                    stream_baru = Stream()
                    stream_baru.entries.append((entry_id, fields))
                    DATA_STORE[key] = (stream_baru, None)

                # Berikan balasan sesuai hasil pengecekan
                if is_valid:
                    # Sukses
                    response = f"${len(entry_id)}\r\n{entry_id}\r\n"
                    connection.sendall(response.encode())

                    # === BARU: Bangunkan klien XREAD yang sedang menunggu Stream ini ===
                    with BLOCK_LOCK:
                        if key in STREAM_BLOCKING_CLIENTS:
                            for bel, base_id in STREAM_BLOCKING_CLIENTS[key]:
                                bel.set() # Tekan belnya!
                else:
                    # Gagal karena melanggar aturan ke-2
                    connection.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

            # ─────────────────────────────────────────
            # PERINTAH: XRANGE → Mengambil data dari Stream dalam rentang tertentu
            # ─────────────────────────────────────────
            elif command == "XRANGE":
                # XRANGE <key> <start> <end>
                key = parts[4]
                start_id = parts[6]
                end_id = parts[8]

                # --- 1. Terjemahkan Batas Awal (Start) ---
                if start_id == "-":
                    # Tanda minus (-) artinya dari paling ujung awal (mulai dari 0-0)
                    start_ms = 0
                    start_seq = 0
                elif "-" in start_id:
                    start_ms, start_seq = map(int, start_id.split("-"))
                else:
                    # Jika tidak ada nomor urut, batas bawah otomatis dimulai dari 0
                    start_ms = int(start_id)
                    start_seq = 0
                
                # --- 2. Terjemahkan Batas Akhir (End) ---
                if end_id == "+":
                    # Tanda plus (+) artinya sampai paling ujung akhir (tak terhingga)
                    end_ms = float('inf')
                    end_seq = float('inf')
                elif "-" in end_id:
                    end_ms, end_seq = map(int, end_id.split("-"))
                else:
                    # Jika tidak ada nomor urut, batas atas dianggap tak terhingga (infinity)
                    end_ms = int(end_id)
                    end_seq = float('inf')

                # Siapkan daftar untuk menampung hasil
                results = []

                # Cek apakah streamnya ada di gudang
                if key in DATA_STORE and isinstance(DATA_STORE[key][0], Stream):
                    stream = DATA_STORE[key][0]
                    # Loop semua data di dalam stream tersebut
                    for entry_id, fields in stream.entries:
                        entry_ms, entry_seq = map(int, entry_id.split("-"))

                        # Cek apakah data ini masuk batas bawah (>= start)
                        valid_start = (entry_ms > start_ms) or (entry_ms == start_ms and entry_seq >= start_seq)
                        
                        # Cek apakah data ini masuk batas atas (<= end)
                        valid_end = (entry_ms < end_ms) or (entry_ms == end_ms and entry_seq <= end_seq)

                        # Kalau masuk dua-duanya, simpan ke hasil!
                        if valid_start and valid_end:
                            results.append((entry_id, fields))
                
                # --- 3. Balas ke Klien (Berbentuk Nested Array) ---
                response = f"*{len(results)}\r\n"
                for entry_id, fields in results:
                    response += "*2\r\n" # Setiap entri terdiri dari 2 bagian: ID dan datanya
                    response += f"${len(entry_id)}\r\n{entry_id}\r\n"
                    
                    # Jumlah elemen di array data adalah jumlah pasangan dikali 2 (kunci dan nilai)
                    response += f"*{len(fields) * 2}\r\n"
                    for f_key, f_val in fields.items():
                        response += f"${len(f_key)}\r\n{f_key}\r\n"
                        response += f"${len(f_val)}\r\n{f_val}\r\n"
                
                connection.sendall(response.encode())

            # ─────────────────────────────────────────
            # PERINTAH: XREAD → Membaca stream mulai dari ID tertentu (Eksklusif)
            # ─────────────────────────────────────────
            elif command == "XREAD":
                # Kita ubah semua bagian menjadi huruf besar untuk memudahkan pencarian kata BLOCK dan STREAMS
                upper_parts = [p.upper() for p in parts]
                
                # --- 1. Cek apakah ada opsi BLOCK ---
                is_blocking = False
                timeout_ms = 0
                if "BLOCK" in upper_parts:
                    is_blocking = True
                    block_idx = upper_parts.index("BLOCK")
                    timeout_ms = int(parts[block_idx + 2])
                
                # --- 2. Cari argumen STREAMS ---
                streams_idx = upper_parts.index("STREAMS")
                args = []
                for i in range(streams_idx + 2, len(parts) - 1, 2):
                    args.append(parts[i])
                
                num_streams = len(args) // 2
                keys = args[:num_streams]
                raw_ids = args[num_streams:]

                # --- 3. Terjemahkan Simbol Dolar ($) ---
                # Simbol $ berarti "ID terakhir yang ada di stream saat perintah ini dipanggil"
                ids = []
                for k, raw_id in zip(keys, raw_ids):
                    if raw_id == "$":
                        if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream) and len(DATA_STORE[k][0].entries) > 0:
                            actual_id = DATA_STORE[k][0].entries[-1][0] # Ambil ID paling belakang
                        else:
                            actual_id = "0-0" # Kalau stream masih kosong, anggap batasnya 0-0
                        ids.append(actual_id)
                    else:
                        ids.append(raw_id)

                # --- Fungsi Pembantu: Mengambil Data ---
                # Karena kita mungkin butuh mencari data 2 kali (sebelum nunggu & sesudah bangun)
                def get_xread_data():
                    resp = []
                    for k, base_id in zip(keys, ids):
                        base_ms, base_seq = map(int, base_id.split("-"))
                        if k in DATA_STORE and isinstance(DATA_STORE[k][0], Stream):
                            stream = DATA_STORE[k][0]
                            entries_found = []
                            for entry_id, fields in stream.entries:
                                entry_ms, entry_seq = map(int, entry_id.split("-"))
                                # Aturan XREAD: EKSKLUSIF (Hanya yang LEBIH BESAR)
                                if (entry_ms > base_ms) or (entry_ms == base_ms and entry_seq > base_seq):
                                    entries_found.append((entry_id, fields))
                            if entries_found:
                                resp.append((k, entries_found))
                    return resp

                # --- Fungsi Pembantu: Menyusun Balasan ---
                def format_xread_response(resp_streams):
                    res = f"*{len(resp_streams)}\r\n"
                    for k, entries in resp_streams:
                        res += "*2\r\n"
                        res += f"${len(k)}\r\n{k}\r\n"
                        res += f"*{len(entries)}\r\n"
                        for entry_id, fields in entries:
                            res += "*2\r\n"
                            res += f"${len(entry_id)}\r\n{entry_id}\r\n"
                            res += f"*{len(fields) * 2}\r\n"
                            for f_key, f_val in fields.items():
                                res += f"${len(f_key)}\r\n{f_key}\r\n"
                                res += f"${len(f_val)}\r\n{f_val}\r\n"
                    return res

                # --- LAKUKAN PENGECEKAN PERTAMA ---
                response_streams = get_xread_data()

                if response_streams:
                    # Kalau saat ini sudah ada datanya, langsung berikan!
                    connection.sendall(format_xread_response(response_streams).encode())
                
                elif is_blocking:
                    # Kalau tidak ada data TAPI klien minta BLOCK (Tunggu)
                    bel = threading.Event()
                    
                    # Daftarkan diri (bel) ke semua stream yang ditunggu
                    with BLOCK_LOCK:
                        for k, base_id in zip(keys, ids):
                            if k not in STREAM_BLOCKING_CLIENTS:
                                STREAM_BLOCKING_CLIENTS[k] = []
                            STREAM_BLOCKING_CLIENTS[k].append((bel, base_id))
                    
                    # Menunggu sampai dibunyikan oleh XADD atau kehabisan waktu
                    if timeout_ms > 0:
                        woke_up = bel.wait(timeout_ms / 1000.0) # Konversi ke detik
                    else:
                        bel.wait() # 0 artinya tunggu tanpa batas waktu
                        woke_up = True
                    
                    # Setelah bangun, jangan lupa cabut pendaftaran biar rapi
                    with BLOCK_LOCK:
                        for k, base_id in zip(keys, ids):
                            if k in STREAM_BLOCKING_CLIENTS:
                                STREAM_BLOCKING_CLIENTS[k] = [
                                    item for item in STREAM_BLOCKING_CLIENTS[k] 
                                    if item[0] != bel
                                ]
                    
                    # Cek hasil setelah bangun
                    if woke_up:
                        # Bangun karena XADD: Cek ulang datanya!
                        response_streams_after = get_xread_data()
                        if response_streams_after:
                            connection.sendall(format_xread_response(response_streams_after).encode())
                        else:
                            connection.sendall(b"*-1\r\n")
                    else:
                        # Waktu habis (Timeout), tidak ada yang nge-bel
                        connection.sendall(b"*-1\r\n")
                
                else:
                    # Kalau tidak ada data dan tidak minta nunggu
                    connection.sendall(b"*-1\r\n")



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
