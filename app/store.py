import threading
import time

# Gudang Data Utama
DATA_STORE = {}

# Pelacakan Versi Kunci (untuk WATCH)
KEY_VERSIONS = {}
GLOBAL_MOD_COUNT = 0

# Gembok Global untuk Keamanan Thread
BLOCK_LOCK = threading.Lock()

# Antrean Blocking
BLOCKING_CLIENTS = {}
STREAM_BLOCKING_CLIENTS = {}

# Konfigurasi Replication
ROLE = "master"
MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
MASTER_REPL_OFFSET = 0
# Hex file RDB kosong (standar Redis)
EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e34fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff10aa325d05474147"

# Daftar koneksi Slave yang aktif (untuk propagasi)
REPLICAS = []

# Offset khusus untuk Slave (jumlah byte yang diterima dari Master)
REPLICA_OFFSET = 0

# Pencatatan offset masing-masing Slave (untuk Master)
# Format: { koneksi: offset_terakhir }
REPLICA_OFFSETS = {}

# Offset total yang sudah dikeluarkan Master (untuk propagasi)
MASTER_REPL_OFFSET = 0

# Konfigurasi Server (RDB Persistence)
CONFIG = {
    "dir": ".",
    "dbfilename": "dump.rdb"
}

class Stream:
    def __init__(self):
        self.entries = []

def touch_key(key):
    """Menandai kunci telah berubah (untuk Optimistic Locking)"""
    global GLOBAL_MOD_COUNT
    with BLOCK_LOCK:
        GLOBAL_MOD_COUNT += 1
        KEY_VERSIONS[key] = GLOBAL_MOD_COUNT

def get_key_version(key):
    return KEY_VERSIONS.get(key, 0)
