import app.store as store

def handle_zset(cmd_name, cmd_parts, target, session=None):
    """Handles Redis Sorted Set commands: ZADD, ZRANGE, ZRANK, ZSCORE."""
    from app.replication import propagate_command
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "ZADD":
        key = arg(1)
        # Supports: ZADD key score member [score member ...]
        if key not in store.DATA_STORE:
            store.DATA_STORE[key] = (store.SortedSet(), None)
        
        zset, _ = store.DATA_STORE[key]
        if not isinstance(zset, store.SortedSet):
            target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return True
            
        added = 0
        for i in range(2, len(cmd_parts), 2):
            score, member = float(cmd_parts[i]), cmd_parts[i+1]
            zset.add(member, score)
            added += 1
            
        store.touch_key(key)
        target.sendall(f":{added}\r\n".encode())
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "ZRANGE":
        key, start, stop = arg(1), int(arg(2)), int(arg(3))
        if key not in store.DATA_STORE:
            target.sendall(b"*0\r\n")
        else:
            zset, _ = store.DATA_STORE[key]
            if not isinstance(zset, store.SortedSet):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                results = zset.get_range_by_rank(start, stop)
                res = f"*{len(results)}\r\n"
                for member in results:
                    res += f"${len(member)}\r\n{member}\r\n"
                target.sendall(res.encode())
        return True

    elif cmd_name == "ZRANK":
        key, member = arg(1), arg(2)
        if key not in store.DATA_STORE:
            target.sendall(b"$-1\r\n")
        else:
            zset, _ = store.DATA_STORE[key]
            if not isinstance(zset, store.SortedSet):
                target.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                # Rank is 0-based index in sorted list
                sorted_items = sorted(zset.elements.items(), key=lambda x: (x[1], x[0]))
                rank = next((i for i, (m, _) in enumerate(sorted_items) if m == member), None)
                if rank is None: target.sendall(b"$-1\r\n")
                else: target.sendall(f":{rank}\r\n".encode())
        return True

    return False
