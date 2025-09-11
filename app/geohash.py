class GeoHash:
    MIN_LATITUDE = -85.05112878
    MAX_LATITUDE = 85.05112878
    MIN_LONGITUDE = -180
    MAX_LONGITUDE = 180

    LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
    LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

    def __init__(self, latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude

    def _spread_int32_to_int64(self, value):
        value &= 0xFFFFFFFF
        value = (value | (value << 16)) & 0x0000FFFF0000FFFF
        value = (value | (value << 8)) & 0x00FF00FF00FF00FF
        value = (value | (value << 4)) & 0x0F0F0F0F0F0F0F0F
        value = (value | (value << 2)) & 0x3333333333333333
        value = (value | (value << 1)) & 0x5555555555555555
        return value
    
    def _interleave(self, x, y):
        x = self._spread_int32_to_int64(x)
        y = self._spread_int32_to_int64(y)
        y_shifted = y << 1
        return x | y_shifted

    def encode(self):
        normalized_latitude = (self.latitude - self.MIN_LATITUDE) / self.LATITUDE_RANGE
        normalized_longitude = (self.longitude - self.MIN_LONGITUDE) / self.LONGITUDE_RANGE

        normalized_latitude = int(normalized_latitude)
        normalized_longitude = int(normalized_longitude)
        return self._interleave(normalized_longitude, normalized_latitude)
