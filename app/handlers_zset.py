import app.store as store

def handle_zset(c, cmd_p, target):
    """Menangani perintah-perintah Sorted Set (ZADD)"""
    from app.replication import propagate_command
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "ZADD":
        # Format: ZADD key score member
        k = arg(1)
        score = arg(2)
        member = arg(3)
        
        if k not in store.DATA_STORE:
            store.DATA_STORE[k] = (store.SortedSet(), None)
        
        zset, _ = store.DATA_STORE[k]
        if not isinstance(zset, store.SortedSet):
            target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return True
            
        added_count = zset.add_member(member, score)
        store.touch_key(k)
        
        # Kirim balasan jumlah member baru
        target.sendall(f":{added_count}\r\n".encode())
        
        # PROPAGASI: Sebarkan ke slave jika kita adalah master
        propagate_command(cmd_p)
        return True
        
    return False
