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
        
        # Simpan koordinat sebagai geohash score (tahap awal: simpan apa adanya)
        # Redis menggunakan Geohash 52-bit, tapi untuk sekarang kita simpan dulu
        from app.geo_utils import geohash_encode
        score = geohash_encode(lon, lat)
        added = zset.add_member(member, score)
        store.touch_key(k)

        target.sendall(f":{added}\r\n".encode())
        return True

    return False
