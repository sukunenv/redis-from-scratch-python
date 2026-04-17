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

import os

# Konfigurasi Server (RDB Persistence & AOF)
CONFIG = {
    "dir": os.getcwd(),
    "dbfilename": "dump.rdb",
    "appendonly": "no",
    "appenddirname": "appendonlydir",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec"
}
AOF_PATH = None

class SortedSet:
    def __init__(self):
        # member -> score
        self.members = {}

    def add_member(self, member, score):
        is_new = member not in self.members
        self.members[member] = float(score)
        return 1 if is_new else 0

    def get_rank(self, member):
        """Return 0-based rank sorted by score asc, then by name lexicographically."""
        if member not in self.members:
            return None
        # Sort: primary = score, secondary = member name (lexicographic)
        sorted_members = sorted(self.members.items(), key=lambda x: (x[1], x[0]))
        for idx, (m, _) in enumerate(sorted_members):
            if m == member:
                return idx
        return None

    def get_sorted(self):
        """Return all members sorted by score asc, then by name."""
        return sorted(self.members.items(), key=lambda x: (x[1], x[0]))

# Pencatatan langganan global (untuk PUBLISH)
# Format: { "nama_channel": set(koneksi1, koneksi2, ...) }
SUBSCRIBERS = {}

class Stream:
    def __init__(self):
        # List of (id, fields_dict)
        self.entries = []

    def add_entry(self, eid, fields):
        # Validasi ID: Harus lebih besar dari entry terakhir
        if self.entries:
            last_id = self.entries[-1][0]
            l_ms, l_seq = map(int, last_id.split("-"))
            c_ms, c_seq = map(int, eid.split("-"))
            if c_ms < l_ms or (c_ms == l_ms and c_seq <= l_seq):
                return False
        self.entries.append((eid, fields))
        return True

    def get_range(self, start, end, exclusive_start=False):
        res = []
        for eid, flds in self.entries:
            # Parsing ID untuk perbandingan
            ms, seq = map(int, eid.split("-"))
            
            # Rentang START
            if start != "-":
                if "-" in start: s_ms, s_seq = map(int, start.split("-"))
                else: s_ms, s_seq = int(start), 0
                if ms < s_ms or (ms == s_ms and seq < s_seq): continue
                if exclusive_start and ms == s_ms and seq == s_seq: continue

            # Rentang END
            if end != "+":
                if "-" in end: e_ms, e_seq = map(int, end.split("-"))
                else: e_ms, e_seq = int(end), float('inf')
                if ms > e_ms or (ms == e_ms and seq > e_seq): continue
            
            res.append((eid, flds))
        return res

def touch_key(key):
    """Menandai kunci telah berubah (untuk Optimistic Locking)"""
    global GLOBAL_MOD_COUNT
    with BLOCK_LOCK:
        GLOBAL_MOD_COUNT += 1
        KEY_VERSIONS[key] = GLOBAL_MOD_COUNT

def get_key_version(key):
    return KEY_VERSIONS.get(key, 0)
