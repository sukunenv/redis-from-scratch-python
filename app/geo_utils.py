import math

def geohash_encode(longitude, latitude):
    """
    Mengubah koordinat (longitude, latitude) menjadi angka Geohash 52-bit.
    Ini cara Redis menyimpan lokasi di dalam Sorted Set.
    
    Bayangkan: dunia dibagi jadi kotak-kotak kecil, dan setiap kotak punya "kode pos" unik.
    Semakin mirip kode pos-nya, semakin dekat lokasinya.
    """
    # Batas koordinat bumi
    lat_range = (-85.05112878, 85.05112878)
    lon_range = (-180.0, 180.0)

    lat_offset = (latitude - lat_range[0]) / (lat_range[1] - lat_range[0])
    lon_offset = (longitude - lon_range[0]) / (lon_range[1] - lon_range[0])

    lat_offset *= (1 << 26)
    lon_offset *= (1 << 26)

    lat_bits = int(lat_offset)
    lon_bits = int(lon_offset)

    # Interleave: selang-seling bit longitude dan latitude
    # Hasilnya adalah satu angka 52-bit yang unik untuk setiap lokasi
    result = 0
    for i in range(26):
        result |= ((lon_bits >> (25 - i)) & 1) << (51 - 2 * i)
        result |= ((lat_bits >> (25 - i)) & 1) << (50 - 2 * i)

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
