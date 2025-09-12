from math import radians, sin, cos, sqrt, atan2

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE


def encode(longitude: float, latitude: float) -> int:
    # Normalize to the range 0-2^26
    normalized_latitude = 2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
    normalized_longitude = 2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

    # Truncate to integers
    normalized_latitude = int(normalized_latitude)
    normalized_longitude = int(normalized_longitude)

    return interleave(normalized_latitude, normalized_longitude)


def interleave(x: int, y: int) -> int:
    x = spread_int32_to_int64(x)
    y = spread_int32_to_int64(y)

    y_shifted = y << 1
    return x | y_shifted


def spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555

    return v


def decode(geo_code: int) -> (float, float): # type: ignore
    """
    decode converts geo code(WGS84) to tuple of (latitude, longitude)
    """
    # Align bits of both latitude and longitude to take even-numbered position
    y = geo_code >> 1
    x = geo_code
    
    # Compact bits back to 32-bit ints
    grid_latitude_number = compact_int64_to_int32(x)
    grid_longitude_number = compact_int64_to_int32(y)
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)


def compact_int64_to_int32(v: int) -> int:
    """
    Compact a 64-bit integer with interleaved bits back to a 32-bit integer.
    This is the reverse operation of spread_int32_to_int64.
    """
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v


def convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number) -> (float, float): # type: ignore
    # Calculate the grid boundaries
    grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / (2**26))
    grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / (2**26))
    grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / (2**26))
    grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / (2**26))
    
    # Calculate the center point of the grid cell
    latitude = (grid_latitude_min + grid_latitude_max) / 2
    longitude = (grid_longitude_min + grid_longitude_max) / 2
    return (longitude, latitude)


def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great-circle distance between two points on the Earth's surface."""
    R = 6372797.560856  # Radius of the Earth in meters

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance