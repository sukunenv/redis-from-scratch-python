import os
import app.store as store

def init_aof():
    """Reads the manifest to determine the active AOF file."""
    if store.CONFIG.get("appendonly") != "yes":
        return
    
    aof_dir = os.path.join(store.CONFIG["dir"], store.CONFIG["appenddirname"])
    manifest_path = os.path.join(aof_dir, f"{store.CONFIG['appendfilename']}.manifest")
    
    if not os.path.exists(manifest_path):
        return
    
    try:
        with open(manifest_path, "r") as f:
            for line in f:
                # Expected format: file <name> seq <n> type i
                parts = line.split()
                if len(parts) >= 6 and parts[0] == "file" and parts[5] == "i":
                    store.AOF_PATH = os.path.join(aof_dir, parts[1])
                    return
    except Exception as e:
        print(f"Error initializing AOF: {e}")

def append_to_aof(command_parts):
    """Appends a write command to the active AOF file in RESP format."""
    if not store.AOF_PATH:
        return
    
    # Encode command to RESP format
    resp = f"*{len(command_parts)}\r\n"
    for part in command_parts:
        resp += f"${len(str(part))}\r\n{part}\r\n"
    
    try:
        with open(store.AOF_PATH, "ab") as f:
            f.write(resp.encode())
            # Synchronize to disk if configured to 'always'
            if store.CONFIG.get("appendfsync") == "always":
                f.flush()
                os.fsync(f.fileno())
    except Exception as e:
        print(f"Error appending to AOF: {e}")

def replay_aof():
    """Reads the AOF file and replays commands to rebuild the database state."""
    if not store.AOF_PATH or not os.path.exists(store.AOF_PATH):
        return
    
    from app.protocol import parse_resp
    from app.handlers import execute_command
    
    class InternalTarget:
        """Dummy target to suppress outputs during command replay."""
        def sendall(self, data): pass
    
    try:
        with open(store.AOF_PATH, "rb") as f:
            data = f.read()
            if not data:
                return
            
            # Use the existing RESP parser to extract commands
            commands = parse_resp(data)
            
            # Disable AOF logging during replay to prevent recursive appends
            original_appendonly = store.CONFIG.get("appendonly")
            store.CONFIG["appendonly"] = "no"
            
            for cmd_parts, _ in commands:
                if cmd_parts:
                    execute_command(cmd_parts, InternalTarget())
            
            # Restore original AOF configuration
            store.CONFIG["appendonly"] = original_appendonly
            
    except Exception as e:
        print(f"Error replaying AOF: {e}")

def rewrite_aof():
    """
    Performs an AOF Rewrite (compaction).
    It generates a minimal set of commands to rebuild the current DATA_STORE state.
    """
    if not store.AOF_PATH:
        return False
    
    temp_path = f"{store.AOF_PATH}.tmp"
    try:
        with open(temp_path, "wb") as f:
            # Snapshot the current data store
            # In a real Redis, this would be a fork()ed child process
            with store.BLOCK_LOCK:
                current_data = list(store.DATA_STORE.items())
            
            for key, (val, expiry) in current_data:
                # 1. Write the SET command for the value
                # This version handles Strings and Lists
                if isinstance(val, (str, bytes)):
                    cmd = ["SET", key, str(val)]
                elif isinstance(val, list):
                    cmd = ["RPUSH", key] + val
                else:
                    # Skip other complex types for now
                    continue
                
                # Format to RESP
                resp = f"*{len(cmd)}\r\n"
                for part in cmd:
                    resp += f"${len(str(part))}\r\n{part}\r\n"
                f.write(resp.encode())
                
                # 2. Write PEXPIREAT if the key has an expiry
                if expiry:
                    exp_cmd = ["PEXPIREAT", key, str(int(expiry * 1000))]
                    resp_exp = f"*{len(exp_cmd)}\r\n"
                    for part in exp_cmd:
                        resp_exp += f"${len(str(part))}\r\n{part}\r\n"
                    f.write(resp_exp.encode())
            
            f.flush()
            os.fsync(f.fileno())
            
        # Atomic swap: Replace old AOF with the compacted one
        os.replace(temp_path, store.AOF_PATH)
        return True
    except Exception as e:
        print(f"Error during AOF rewrite: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

