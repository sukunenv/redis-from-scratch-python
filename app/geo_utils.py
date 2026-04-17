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
    """
    Mengubah Geohash 52-bit kembali menjadi (longitude, latitude).
    Kebalikan dari encode.
    """
    hash_val = int(score)

    lon_bits = 0
    lat_bits = 0
    for i in range(26):
        lon_bits |= ((hash_val >> (51 - 2 * i)) & 1) << (25 - i)
        lat_bits |= ((hash_val >> (50 - 2 * i)) & 1) << (25 - i)

    lat_range = (-85.05112878, 85.05112878)
    lon_range = (-180.0, 180.0)

    longitude = lon_range[0] + (lon_bits / (1 << 26)) * (lon_range[1] - lon_range[0])
    latitude = lat_range[0] + (lat_bits / (1 << 26)) * (lat_range[1] - lat_range[0])

    return longitude, latitude
