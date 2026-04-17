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

def execute_command(cmd_parts, target, session=None):
    """
    Main command dispatcher. Routes commands to the appropriate module handlers.
    Ensures authentication and AOF persistence are handled before execution.
    """
    try:
        import app.store as store
        
        # Initialize default user if not already present
        if not hasattr(store, "USERS"):
            store.USERS = {"default": {"flags": ["nopass"], "passwords": []}}
            
        # Handle automatic authentication for default 'nopass' users
        if session is not None and "authenticated_user" not in session:
            if "nopass" in store.USERS["default"]["flags"]:
                session["authenticated_user"] = "default"
            else:
                session["authenticated_user"] = None
                
        cmd_name = cmd_parts[0].upper()
        
        # Security Gate: Enforce NOAUTH for unauthenticated connections
        if session is not None and session.get("authenticated_user") is None and cmd_name != "AUTH":
            target.sendall(b"-NOAUTH Authentication required.\r\n")
            return

        # AOF Persistence: Record commands that modify data
        WRITE_COMMANDS = {
            "SET", "DEL", "HSET", "XADD", "ZADD", "GEOADD", 
            "INCR", "EXPIRE", "PEXPIRE", "HDEL"
        }
        if cmd_name in WRITE_COMMANDS and store.CONFIG.get("appendonly") == "yes":
            from app.aof import append_to_aof
            append_to_aof(cmd_parts)
            
        # Command Routing
        if handle_generic(cmd_name, cmd_parts, target, session): return
        if handle_pubsub(cmd_name, cmd_parts, target, session): return
        if handle_replication(cmd_name, cmd_parts, target, session): return
        if handle_auth(cmd_name, cmd_parts, target, session): return
        if handle_data(cmd_name, cmd_parts, target, session): return
        if handle_list(cmd_name, cmd_parts, target, session): return
        if handle_stream(cmd_name, cmd_parts, target, session): return
        if handle_zset(cmd_name, cmd_parts, target, session): return
        if handle_geo(cmd_name, cmd_parts, target, session): return

    except Exception as e:
        print(f"Error executing command: {e}")
