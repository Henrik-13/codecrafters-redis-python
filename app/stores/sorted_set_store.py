import bisect
import threading
from app.utils.geohash import haversine, decode as decode_geohash

class _SortedSet:
    def __init__(self):
        self.members = []
        self.scores = {}

    def add(self, score, member):
        if member in self.scores:
            old_score = self.scores[member]
            if old_score == score:
                return 0
            old_entry = (old_score, member)
            index = bisect.bisect_left(self.members, old_entry)
            if index < len(self.members) and self.members[index] == old_entry:
                self.members.pop(index)
            
            is_new = False
        else:
            is_new = True

        new_entry = (score, member)
        bisect.insort_left(self.members, new_entry)
        self.scores[member] = score
        return 1 if is_new else 0
    
    def rank(self, member):
        if member not in self.scores:
            return None
        
        score = self.scores[member]
        entry = (score, member)
        index = bisect.bisect_left(self.members, entry)
        if index < len(self.members) and self.members[index] == entry:
            return index
        return None
            

class SortedSetStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def zadd(self, key, args):
        if len(args) % 2 != 0:
            raise ValueError("wrong number of arguments for 'ZADD' command")

        added_count = 0
        with self.lock:
            if key not in self.data:
                self.data[key] = _SortedSet()
            zset = self.data[key]

            for i in range (0, len(args), 2):
                try:
                    score = float(args[i])
                    member = args[i + 1]
                    added_count += zset.add(score, member)
                except ValueError:
                    ValueError("score is not a valid float")
        return added_count
    
    def zrank(self, key, member):
        with self.lock:
            if key not in self.data:
                return None
            zset = self.data[key]
            return zset.rank(member)
        
    def zrange(self, key, start, end):
        with self.lock:
            if key not in self.data:
                return []
            zset = self.data[key]
            members = [member for score, member in zset.members]

            if start < 0:
                start = len(members) + start
            if end < 0:
                end = len(members) + end

            start = max(0, min(start, len(members)))
            end = max(-1, min(end, len(members) - 1))

            return members[start:end + 1]
        
    def zcard(self, key):
        with self.lock:
            if key not in self.data:
                return 0
            zset = self.data[key]
            return len(zset.members)
        
    def zscore(self, key, member):
        with self.lock:
            if key not in self.data:
                return None
            zset = self.data[key]
            return zset.scores.get(member, None)
        
    def zrem(self, key, member):
        with self.lock:
            if key not in self.data:
                return 0
            zset = self.data[key]
            if member not in zset.scores:
                return 0
            
            score = zset.scores[member]
            entry = (score, member)
            index = bisect.bisect_left(zset.members, entry)
            if index < len(zset.members) and zset.members[index] == entry:
                zset.members.pop(index)
                del zset.scores[member]
                return 1
            return 0
        
    def exists(self, key):
        with self.lock:
            return key in self.data

    def geosearch(self, key, center_lon, center_lat, radius, unit):
        unit_conversions = {
            'm': 1,
            'km': 1000,
            'ft': 0.3048,
            'mi': 1609.34
        }
        if unit.lower() not in unit_conversions:
            raise ValueError(f"unsupported unit provided. please use m, km, ft, mi")
        
        radius_in_meters = radius * unit_conversions[unit.lower()]
        
        matching_locations = []
        with self.lock:
            if key not in self.data:
                return []

            zset = self.data[key]
            for member, score in zset.scores.items():
                lon, lat = decode_geohash(int(score))
                distance = haversine(center_lon, center_lat, lon, lat)
                if distance <= radius_in_meters:
                    matching_locations.append(member)
        
        return matching_locations