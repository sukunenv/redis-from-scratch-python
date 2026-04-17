import os
import threading

# Server Configuration (RDB Persistence & AOF)
CONFIG = {
    "dir": os.getcwd(),
    "dbfilename": "dump.rdb",
    "appendonly": "no",
    "appenddirname": "appendonlydir",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec"
}

# Global path to the active AOF file (determined at startup)
AOF_PATH = None

# Main Data Stores
DATA_STORE = {}
EXPIRY_STORE = {}

# Locks for thread-safe operations
DATA_LOCK = threading.Lock()
BLOCK_LOCK = threading.Lock()

# Pub/Sub Management
SUBSCRIBERS = {}  # {channel_name: set(connection_sockets)}

# Key Versioning for WATCH/MULTI/EXEC transactions
KEY_VERSIONS = {}

class SortedSet:
    """Implements a simple sorted set using a dictionary for scores."""
    def __init__(self):
        self.elements = {}  # {member: score}

    def add(self, member, score):
        self.elements[member] = score

    def get_score(self, member):
        return self.elements.get(member)

    def get_range_by_rank(self, start, stop):
        sorted_items = sorted(self.elements.items(), key=lambda x: (x[1], x[0]))
        if stop == -1: stop = len(sorted_items) - 1
        return [item[0] for item in sorted_items[start:stop + 1]]

class Stream:
    """Implements a Redis Stream-like data structure."""
    def __init__(self):
        self.entries = []  # List of (id, fields_dict)
        self.last_id = "0-0"

    def add(self, entry_id, fields):
        self.entries.append((entry_id, fields))
        self.last_id = entry_id

def touch_key(key):
    """Increments the version of a key to trigger WATCH failures in transactions."""
    KEY_VERSIONS[key] = KEY_VERSIONS.get(key, 0) + 1

