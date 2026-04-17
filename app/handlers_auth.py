def handle_auth(c, cmd_p, target, session):
    """
    Menangani perintah Authentication (ACL, AUTH, dll)
    Ibarat pos satpam untuk ngecek identitas tamu.
    """
    def arg(idx): return cmd_p[idx].upper() if idx < len(cmd_p) else None

    if c == "ACL":
        subcommand = arg(1)
        if subcommand == "WHOAMI":
            # Sesuai instruksi tester: hardcode balasan dengan "default"
            target.sendall(b"$7\r\ndefault\r\n")
            return True
        elif subcommand == "GETUSER":
            # Format: ACL GETUSER username
            # Saat ini kita diminta cuma nampilin properti "flags" berupa Array kosong
            # RESP: Array isinya 2 -> "flags" dan []
            target.sendall(b"*2\r\n$5\r\nflags\r\n*0\r\n")
            return True

    return False
