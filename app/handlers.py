import app.store as store
from app.handlers_pubsub import handle_pubsub
from app.handlers_replication import handle_replication
from app.handlers_data import handle_data
from app.handlers_list import handle_list
from app.handlers_stream import handle_stream

def execute_command(cmd_p, target, session=None):
    """
    Dispatcher Utama: Mengarahkan perintah ke modul handler yang tepat.
    """
    try:
        c = cmd_p[0].upper()
        def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

        # 1. Coba di Departemen Pub/Sub
        if handle_pubsub(c, cmd_p, target, session): return

        # 2. Coba di Departemen Replikasi
        if handle_replication(c, cmd_p, target): return

        # 3. Coba di Departemen Data Umum
        if handle_data(c, cmd_p, target): return

        # 4. Coba di Departemen List
        if handle_list(c, cmd_p, target): return

        # 5. Coba di Departemen Stream
        if handle_stream(c, cmd_p, target): return

        # Perintah khusus yang belum dipecah
        if c == "ECHO":
            val = arg(1) or ""
            target.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        
        else:
            # Jika tidak ada yang kenal perintahnya
            # Untuk Redis asli ini akan kirim -ERR unknown command
            pass

    except Exception as e:
        print(f"Error executing command {cmd_p}: {e}")
