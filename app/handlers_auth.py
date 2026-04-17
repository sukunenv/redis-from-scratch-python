import app.store as store
import hashlib

def handle_auth(c, cmd_p, target, session):
    """
    Menangani perintah Authentication (ACL, AUTH, dll)
    Ibarat pos satpam untuk ngecek identitas tamu.
    """
    # Inisialisasi buku tamu (database user) jika belum ada
    if not hasattr(store, "USERS"):
        store.USERS = {
            "default": {
                "flags": ["nopass"],
                "passwords": []
            }
        }

    def arg(idx): return cmd_p[idx].upper() if idx < len(cmd_p) else None

    if c == "ACL":
        subcommand = arg(1)
        if subcommand == "WHOAMI":
            target.sendall(b"$7\r\ndefault\r\n")
            return True
            
        elif subcommand == "GETUSER":
            # Ambil data user dari buku tamu
            username = cmd_p[2] if len(cmd_p) > 2 else "default"
            user = store.USERS.get(username, {"flags": [], "passwords": []})
            
            flags = user["flags"]
            passwords = user["passwords"]
            
            # Bangun respon RESP secara dinamis
            res = "*4\r\n$5\r\nflags\r\n"
            res += f"*{len(flags)}\r\n"
            for f in flags:
                res += f"${len(f)}\r\n{f}\r\n"
                
            res += "$9\r\npasswords\r\n"
            res += f"*{len(passwords)}\r\n"
            for p in passwords:
                res += f"${len(p)}\r\n{p}\r\n"
                
            target.sendall(res.encode())
            return True
            
        elif subcommand == "SETUSER":
            # Format: ACL SETUSER username [rules...]
            username = cmd_p[2]
            if username not in store.USERS:
                store.USERS[username] = {"flags": [], "passwords": []}
                
            user = store.USERS[username]
            
            # Proses aturan-aturan (rules) yang diberikan
            for rule in cmd_p[3:]:
                # Jika rule diawali dengan '>' berarti itu instruksi penambahan password
                if rule.startswith(">"):
                    password = rule[1:]
                    # Hash password dengan SHA-256 (Keamanan tingkat dewa)
                    hashed = hashlib.sha256(password.encode()).hexdigest()
                    if hashed not in user["passwords"]:
                        user["passwords"].append(hashed)
                        
                    # Cabut stiker 'nopass' karena user ini sekarang punya password
                    if "nopass" in user["flags"]:
                        user["flags"].remove("nopass")
                        
            target.sendall(b"+OK\r\n")
            return True

    return False
