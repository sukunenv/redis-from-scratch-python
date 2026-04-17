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
