import math

def geohash_encode(longitude, latitude):
    lat_range = [-85.05112878, 85.05112878]
    lon_range = [-180.0, 180.0]

    result = 0
    for step in range(26):
        # Longitude bit
        lon_mid = (lon_range[0] + lon_range[1]) / 2
        if longitude >= lon_mid:
            result = (result << 1) | 1
            lon_range[0] = lon_mid
        else:
            result = (result << 1) | 0
            lon_range[1] = lon_mid

        # Latitude bit
        lat_mid = (lat_range[0] + lat_range[1]) / 2
        if latitude >= lat_mid:
            result = (result << 1) | 1
            lat_range[0] = lat_mid
        else:
            result = (result << 1) | 0
            lat_range[1] = lat_mid

    return float(result)


def geohash_decode(score):
    hash_val = int(score)
    lat_range = [-85.05112878, 85.05112878]
    lon_range = [-180.0, 180.0]

    for step in range(26):
        # Ambil bit Longitude di posisi (51 - 2*step)
        lon_bit = (hash_val >> (51 - 2 * step)) & 1
        lon_mid = (lon_range[0] + lon_range[1]) / 2.0
        if lon_bit:
            lon_range[0] = lon_mid
        else:
            lon_range[1] = lon_mid

        # Ambil bit Latitude di posisi (50 - 2*step)
        lat_bit = (hash_val >> (50 - 2 * step)) & 1
        lat_mid = (lat_range[0] + lat_range[1]) / 2.0
        if lat_bit:
            lat_range[0] = lat_mid
        else:
            lat_range[1] = lat_mid

    # Koordinat akhir adalah titik tengah dari sisa range
    longitude = (lon_range[0] + lon_range[1]) / 2.0
    latitude = (lat_range[0] + lat_range[1]) / 2.0

    return longitude, latitude
