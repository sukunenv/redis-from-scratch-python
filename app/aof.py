import os
import app.store as store

def init_aof():
    """Membaca manifest untuk menentukan file AOF mana yang aktif"""
    if store.CONFIG.get("appendonly") != "yes":
        return
    
    aof_dir = os.path.join(store.CONFIG["dir"], store.CONFIG["appenddirname"])
    manifest_path = os.path.join(aof_dir, f"{store.CONFIG['appendfilename']}.manifest")
    
    if not os.path.exists(manifest_path):
        return
    
    with open(manifest_path, "r") as f:
        for line in f:
            # Format: file <name> seq <n> type i
            parts = line.split()
            if len(parts) >= 6 and parts[0] == "file" and parts[5] == "i":
                store.AOF_PATH = os.path.join(aof_dir, parts[1])
                return

def append_to_aof(command_parts):
    """Menulis perintah ke file AOF dalam format RESP"""
    if not store.AOF_PATH:
        return
    
    # Encode ke RESP
    # Contoh: SET foo 100 -> *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n
    resp = f"*{len(command_parts)}\r\n"
    for part in command_parts:
        resp += f"${len(str(part))}\r\n{part}\r\n"
    
    # Gunakan mode "ab" (append binary)
    try:
        with open(store.AOF_PATH, "ab") as f:
            f.write(resp.encode())
            if store.CONFIG.get("appendfsync") == "always":
                f.flush()
                os.fsync(f.fileno())
    except Exception as e:
        print(f"Gagal menulis ke AOF: {e}")
