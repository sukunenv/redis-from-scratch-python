import app.store as store

def handle_geo(c, cmd_p, target):
    """Menangani perintah-perintah Geospatial (GEOADD, dll)"""
    def arg(idx): return cmd_p[idx] if idx < len(cmd_p) else None

    if c == "GEOADD":
        # Format: GEOADD key longitude latitude member
        k = arg(1)
        lon = float(arg(2))
        lat = float(arg(3))
        member = arg(4)

        # Validasi Longitude dan Latitude
        is_lon_valid = -180.0 <= lon <= 180.0
        is_lat_valid = -85.05112878 <= lat <= 85.05112878

        if not is_lon_valid or not is_lat_valid:
            target.sendall(f"-ERR invalid longitude,latitude pair {lon},{lat}\r\n".encode())
            return True

        # Di Redis, GEOADD menyimpan data di Sorted Set
        # dengan "geohash" sebagai score-nya.
        # Untuk tahap awal, kita simpan sederhana dulu.
        if k not in store.DATA_STORE:
            store.DATA_STORE[k] = (store.SortedSet(), None)

        zset, _ = store.DATA_STORE[k]
        
        # Sekarang kita sudah boleh pakai Geohash asli!
        from app.geo_utils import geohash_encode
        score = geohash_encode(lon, lat)
        
        added = zset.add_member(member, score)
        store.touch_key(k)

        target.sendall(f":{added}\r\n".encode())
        return True

    elif c == "GEOPOS":
        # Format: GEOPOS key member [member ...]
        k = arg(1)
        members = cmd_p[2:]
        
        # Respons utama adalah sebuah Array yang isinya sebanyak member yang ditanya
        res = f"*{len(members)}\r\n"
        
        zset = None
        if k in store.DATA_STORE:
            val, _ = store.DATA_STORE[k]
            if isinstance(val, store.SortedSet):
                zset = val
                
        from app.geo_utils import geohash_decode
        
        for member in members:
            if zset and member in zset.members:
                # Ambil score asli (angka raksasa) dan decode!
                score = zset.members[member]
                lon, lat = geohash_decode(score)
                
                lon_str = str(lon)
                lat_str = str(lat)
                
                res += f"*2\r\n${len(lon_str)}\r\n{lon_str}\r\n${len(lat_str)}\r\n{lat_str}\r\n"
            else:
                # Member tidak ada (atau key tidak ada). Kirim null array.
                res += "*-1\r\n"
                
        target.sendall(res.encode())
        return True

    return False
