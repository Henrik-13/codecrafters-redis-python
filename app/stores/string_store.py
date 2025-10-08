import threading


class StringStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def set(self, key, value, px=None):
        with self.lock:
            self.data[key] = value
            if px is not None:
                threading.Timer(px / 1000.0, self.delete, args=[key]).start()

    def get(self, key):
        with self.lock:
            return self.data.get(key, None)

    def incr(self, key):
        with self.lock:
            if key not in self.data:
                self.data[key] = "1"
                return 1
            try:
                value = int(self.data[key])
                value += 1
                self.data[key] = str(value)
                return value
            except ValueError as e:
                raise ValueError("Value is not an integer or out of range") from e

    def keys(self):
        with self.lock:
            return list(self.data.keys())

    def load_from_rdb(self, rdb_data):
        with self.lock:
            self.data = rdb_data

    def delete(self, key):
        with self.lock:
            self.data.pop(key, None)
