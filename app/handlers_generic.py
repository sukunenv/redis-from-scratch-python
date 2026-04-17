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
    
    return False
