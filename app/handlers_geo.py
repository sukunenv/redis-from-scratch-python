import app.store as store
from app.geo_utils import haversine_distance

def handle_geo(cmd_name, cmd_parts, target, session=None):
    """Handles Redis Geospatial commands: GEOADD, GEODIST."""
    from app.replication import propagate_command
    def arg(idx): return cmd_parts[idx] if idx < len(cmd_parts) else None

    if cmd_name == "GEOADD":
        key = arg(1)
        # Supports: GEOADD key longitude latitude member [longitude latitude member ...]
        if key not in store.DATA_STORE:
            store.DATA_STORE[key] = (store.SortedSet(), None)
            
        zset, _ = store.DATA_STORE[key]
        added = 0
        for i in range(2, len(cmd_parts), 3):
            lon, lat, member = float(cmd_parts[i]), float(cmd_parts[i+1]), cmd_parts[i+2]
            # In a real Redis, GEOADD converts to a geohash score. 
            # Here we simplify by storing coordinates as a tuple/string score or separate meta.
            # However, for GEODIST to work, we'll store (lon, lat) in the sorted set's score field (as a tuple).
            zset.add(member, (lon, lat))
            added += 1
            
        store.touch_key(key)
        target.sendall(f":{added}\r\n".encode())
        propagate_command(cmd_parts)
        return True

    elif cmd_name == "GEODIST":
        key, m1, m2 = arg(1), arg(2), arg(3)
        unit = arg(4) or "m"
        
        if key not in store.DATA_STORE:
            target.sendall(b"$-1\r\n")
        else:
            zset, _ = store.DATA_STORE[key]
            p1 = zset.get_score(m1)
            p2 = zset.get_score(m2)
            
            if not p1 or not p2:
                target.sendall(b"$-1\r\n")
            else:
                dist = haversine_distance(p1[1], p1[0], p2[1], p2[0])
                # Convert units (m, km, mi, ft)
                if unit == "km": dist /= 1000.0
                elif unit == "mi": dist /= 1609.34
                elif unit == "ft": dist /= 0.3048
                
                res = f"{dist:.4f}"
                target.sendall(f"${len(res)}\r\n{res}\r\n".encode())
        return True

    return False
