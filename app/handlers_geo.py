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

    elif c == "GEODIST":
        # Format: GEODIST key member1 member2 [unit]
        k = arg(1)
        m1 = arg(2)
        m2 = arg(3)
        # arg(4) adalah unit (m, km, ft, mi). Di tes ini kita pakai default (meter)
        
        if k not in store.DATA_STORE:
            target.sendall(b"$-1\r\n")
            return True

        val, _ = store.DATA_STORE[k]
        if not isinstance(val, store.SortedSet) or m1 not in val.members or m2 not in val.members:
            target.sendall(b"$-1\r\n")
            return True

        from app.geo_utils import geohash_decode, haversine_distance
        
        score1 = val.members[m1]
        score2 = val.members[m2]
        
        lon1, lat1 = geohash_decode(score1)
        lon2, lat2 = geohash_decode(score2)
        
        dist = haversine_distance(lon1, lat1, lon2, lat2)
        dist_str = f"{dist:.4f}"
        
        target.sendall(f"${len(dist_str)}\r\n{dist_str}\r\n".encode())
        return True

    elif c == "GEOSEARCH":
        # Format: GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit
        k = arg(1)
        
        lon_center = 0.0
        lat_center = 0.0
        radius = 0.0
        unit = "m"
        
        # Parsing argumen yang fleksibel (berdasarkan perintah Redis)
        i = 2
        while i < len(cmd_p):
            token = str(cmd_p[i]).upper()
            if token == "FROMLONLAT":
                lon_center = float(cmd_p[i+1])
                lat_center = float(cmd_p[i+2])
                i += 3
            elif token == "BYRADIUS":
                radius = float(cmd_p[i+1])
                unit = str(cmd_p[i+2]).lower()
                i += 3
            else:
                i += 1
                
        # Konversi radius ke satuan meter
        if unit == "km": radius *= 1000.0
        elif unit == "mi": radius *= 1609.34
        elif unit == "ft": radius *= 0.3048
        
        results = []
        if k in store.DATA_STORE:
            val, _ = store.DATA_STORE[k]
            if isinstance(val, store.SortedSet):
                from app.geo_utils import geohash_decode, haversine_distance
                # Cek satu-satu mana yang masuk ke dalam radar kita
                for member, score in val.members.items():
                    m_lon, m_lat = geohash_decode(score)
                    dist = haversine_distance(m_lon, m_lat, lon_center, lat_center)
                    if dist <= radius:
                        results.append(member)
                        
        # Kirim hasil berbentuk Array
        res = f"*{len(results)}\r\n"
        for r in results:
            res += f"${len(r)}\r\n{r}\r\n"
            
        target.sendall(res.encode())
        return True

    return False
