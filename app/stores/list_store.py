import threading

class ListStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def lpush(self, key, values):
        with self.lock:
            if key not in self.data:
                self.data[key] = []
            for value in values:
                self.data[key].insert(0, value)
            return len(self.data[key])

    def rpush(self, key, values):
        with self.lock:
            if key not in self.data:
                self.data[key] = []
            for value in values:
                self.data[key].append(value)
            return len(self.data[key])

    def lpop(self, key, count=1):
        with self.lock:
            if key not in self.data or not self.data[key]:
                return None

            popped_items = []
            for _ in range(min(count, len(self.data[key]))):
                popped_items.append(self.data[key].pop(0))
            
            return popped_items

    def lrange(self, key, start, end):
        with self.lock:
            if key not in self.data:
                return []

            lst = self.data[key]
            if start < 0:
                start = len(lst) + start
            if end < 0:
                end = len(lst) + end

            start = max(0, min(start, len(lst)))
            end = max(-1, min(end, len(lst) - 1))

            return lst[start:end + 1]

    def llen(self, key):
        with self.lock:
            return len(self.data.get(key, []))
        
    def exists(self, key):
        with self.lock:
            return key in self.data