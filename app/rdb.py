import os
import struct
import app.store as store

def load_rdb():
    """Membaca file RDB dan memuat datanya ke DATA_STORE"""
    filepath = os.path.join(store.CONFIG["dir"], store.CONFIG["dbfilename"])
    if not os.path.exists(filepath):
        return
    
    try:
        with open(filepath, "rb") as f:
            # 1. Header (REDIS0011) - 9 bytes pertama
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
                elif op == b"\xFC": # Expiry dalam milidetik (8 bytes)
                    expiry_ms = struct.unpack("<Q", f.read(8))[0]
                    _read_kv(f, expiry_ms / 1000.0)
                elif op == b"\xFD": # Expiry dalam detik (4 bytes)
                    expiry_s = struct.unpack("<I", f.read(4))[0]
                    _read_kv(f, float(expiry_s))
                else:
                    # Pasangan Key-Value normal (op adalah Tipe Data)
                    _read_kv(f, None, op)
    except Exception as e:
        print(f"Gagal memuat file RDB: {e}")

def _read_length(f):
    """Membaca panjang data sesuai aturan encoding RDB"""
    b = f.read(1)
    if not b: return 0, False
    first = b[0]
    enc = (first & 0xC0) >> 6
    
    if enc == 0: # 6 bit berikutnya adalah panjangnya
        return first & 0x3F, False
    elif enc == 1: # 14 bit (6 bit sekarang + 8 bit byte berikutnya)
        second = f.read(1)[0]
        return ((first & 0x3F) << 8) | second, False
    elif enc == 2: # 32 bit (4 byte berikutnya)
        return struct.unpack(">I", f.read(4))[0], False
    else: # Special encoding (biasanya angka yang disimpan jadi string)
        return first & 0x3F, True

def _read_string(f):
    """Membaca string dari file RDB"""
    length, is_special = _read_length(f)
    if is_special:
        # Spesial: Angka 8-bit, 16-bit, atau 32-bit
        if length == 0: return str(f.read(1)[0])
        if length == 1: return str(struct.unpack("<H", f.read(2))[0])
        if length == 2: return str(struct.unpack("<I", f.read(4))[0])
        return ""
    # String normal
    return f.read(length).decode(errors="ignore")

def _read_kv(f, expiry, type_byte=None):
    """Membaca pasangan Key-Value dan menyimpannya ke memori"""
    t = type_byte if type_byte else f.read(1)
    if t == b"\x00": # Tipe: String
        key = _read_string(f)
        val = _read_string(f)
        store.DATA_STORE[key] = (val, expiry)
