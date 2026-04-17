import app.store as store
from app.handlers_pubsub import handle_pubsub
from app.handlers_replication import handle_replication
from app.handlers_data import handle_data
from app.handlers_list import handle_list
from app.handlers_stream import handle_stream
from app.handlers_zset import handle_zset
from app.handlers_generic import handle_generic
from app.handlers_geo import handle_geo
from app.handlers_auth import handle_auth

def execute_command(cmd_p, target, session=None):
    """
    Dispatcher Utama: Mengarahkan perintah ke modul handler yang tepat.
    """
    try:
        import app.store as store
        if not hasattr(store, "USERS"):
            store.USERS = {"default": {"flags": ["nopass"], "passwords": []}}
            
        # Jika tamu baru datang, cek apakah dia boleh langsung masuk (nopass)
        if session is not None and "authenticated_user" not in session:
            if "nopass" in store.USERS["default"]["flags"]:
                session["authenticated_user"] = "default"
            else:
                session["authenticated_user"] = None
                
        c = cmd_p[0].upper()
        
        # Gerbang Elektronik: Usir tamu yang belum login (kecuali dia mau login pakai AUTH)
        if session is not None and session.get("authenticated_user") is None and c != "AUTH":
            target.sendall(b"-NOAUTH Authentication required.\r\n")
            return

        # Pencatatan AOF (Tukang Catat): Rekam perintah yang mengubah data
        WRITE_COMMANDS = {"SET", "DEL", "HSET", "XADD", "ZADD", "GEOADD", "INCR", "EXPIRE", "PEXPIRE", "HDEL"}
        if c in WRITE_COMMANDS and store.CONFIG.get("appendonly") == "yes":
            from app.aof import append_to_aof
            append_to_aof(cmd_p)
            
        def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

        # 0. Coba di Departemen Dasar (PING, ECHO)
        if handle_generic(c, cmd_p, target, session): return

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

        # 6. Coba di Departemen Sorted Set
        if handle_zset(c, cmd_p, target): return

        # 7. Coba di Departemen Geospatial
        if handle_geo(c, cmd_p, target): return

        # 8. Coba di Departemen Auth (Keamanan)
        if handle_auth(c, cmd_p, target, session): return

    except Exception as e:
        print(f"Error executing command {cmd_p}: {e}")
