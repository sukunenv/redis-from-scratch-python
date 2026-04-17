import os
import struct
import app.store as store

def load_rdb():
    """Reads the RDB snapshot file and loads data into DATA_STORE."""
    filepath = os.path.join(store.CONFIG["dir"], store.CONFIG["dbfilename"])
    if not os.path.exists(filepath):
        return
    
    try:
        with open(filepath, "rb") as f:
            # 1. Header (REDIS0011) - first 9 bytes
            header = f.read(9)
            if not header.startswith(b"REDIS"): return
            
            while True:
                op = f.read(1)
                if not op: break
                
                if op == b"\xFA": # Auxiliary fields (Metadata)
                    _read_string(f) # Key
                    _read_string(f) # Value
                elif op == b"\xFE": # Select DB
                    _read_length(f)
                elif op == b"\xFB": # Resize DB (Hash Table info)
                    _read_length(f) # Total keys
                    _read_length(f) # Expiry keys
                elif op == b"\xFF": # End of File
                    break
                elif op == b"\xFC": # Expiry in milliseconds (8 bytes)
                    expiry_ms = struct.unpack("<Q", f.read(8))[0]
                    _read_kv(f, expiry_ms / 1000.0)
                elif op == b"\xFD": # Expiry in seconds (4 bytes)
                    expiry_s = struct.unpack("<I", f.read(4))[0]
                    _read_kv(f, float(expiry_s))
                else:
                    # Standard Key-Value pair (op is the Value Type)
                    _read_kv(f, None, op)
    except Exception as e:
        print(f"Error loading RDB file: {e}")

def _read_length(f):
    """Parses RDB length-encoded data according to Redis specification."""
    b = f.read(1)
    if not b: return 0, False
    first = b[0]
    enc = (first & 0xC0) >> 6
    
    if enc == 0: # 6-bit length
        return first & 0x3F, False
    elif enc == 1: # 14-bit length (6 bits now + 8 bits from next byte)
        second = f.read(1)[0]
        return ((first & 0x3F) << 8) | second, False
    elif enc == 2: # 32-bit length (next 4 bytes)
        return struct.unpack(">I", f.read(4))[0], False
    else: # Special encoding (often integers stored as strings)
        return first & 0x3F, True

def _read_string(f):
    """Parses a string from the RDB file."""
    length, is_special = _read_length(f)
    if is_special:
        # Special: 8-bit, 16-bit, or 32-bit integer encodings
        if length == 0: return str(f.read(1)[0])
        if length == 1: return str(struct.unpack("<H", f.read(2))[0])
        if length == 2: return str(struct.unpack("<I", f.read(4))[0])
        return ""
    # Standard byte string
    return f.read(length).decode(errors="ignore")

def _read_kv(f, expiry, type_byte=None):
    """Reads a Key-Value pair and stores it in the memory data store."""
    t = type_byte if type_byte else f.read(1)
    if t == b"\x00": # Type: String
        key = _read_string(f)
        val = _read_string(f)
        store.DATA_STORE[key] = (val, expiry)
