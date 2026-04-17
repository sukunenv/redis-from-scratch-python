import app.store as store

def handle_generic(c, cmd_p, target, session):
    """Menangani perintah dasar (PING, ECHO)"""
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "PING":
        if session is not None and len(session["subscribed_channels"]) > 0:
            # Respons khusus PING saat dalam mode langganan
            target.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
        else:
            target.sendall(b"+PONG\r\n")
        return True

    elif c == "ECHO":
        val = arg(1) or ""
        target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        return True

    elif c == "CONFIG":
        # Mengambil informasi konfigurasi server
        sub = arg(1).upper() if arg(1) else ""
        if sub == "GET":
            param = arg(2).lower() if arg(2) else ""
            val = store.CONFIG.get(param, "")
            # Format RESP: Array berisi 2 elemen [nama_param, nilai_param]
            res = f"*2\r\n${len(param)}\r\n{param}\r\n${len(val)}\r\n{val}\r\n"
            target.sendall(res.encode())
        return True
    
    return False
