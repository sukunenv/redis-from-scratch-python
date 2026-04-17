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
