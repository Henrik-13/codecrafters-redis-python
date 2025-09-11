import time
import threading

class StreamStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def _parse_id(self, id):
        parts = id.split("-")
        if len(parts) != 2:
            return None, None
        try:
            timestamp = int(parts[0])
            sequence = int(parts[1]) if parts[1] != "*" else "*"
            return timestamp, sequence
        except ValueError:
            return None, None
        
    def _compare_ids(self, id1, id2):
        ts1, seq1 = self._parse_id(id1)
        ts2, seq2 = self._parse_id(id2)
        if ts1 < ts2:
            return -1
        if ts1 > ts2:
            return 1
        if seq1 < seq2:
            return -1
        if seq1 > seq2:
            return 1
        return 0

    def _validate_id(self, key, stream_id):
        new_ts, new_seq = self._parse_id(stream_id)
        if new_ts is None or new_seq is None or new_ts < 0 or new_seq < 0:
            return False
        if new_ts == 0 and new_seq == 0:
            return False
        if key not in self.data or not self.data[key]:
            return True
        last_id = self.data[key][-1]["id"]
        return self._compare_ids(stream_id, last_id) > 0

    def _generate_id(self, key, stream_id):
        ts, seq = self._parse_id(stream_id)
        if ts is None: return None

        if seq == "*":
            base_ts = ts if ts is not None else int(time.time() * 1000)
            if key not in self.data or not self.data[key]:
                seq_num = 1 if base_ts == 0 else 0
                return f"{base_ts}-{seq_num}"
            
            last_id = self.data[key][-1]["id"]
            last_ts, last_seq = self._parse_id(last_id)
            if base_ts > last_ts:
                return f"{base_ts}-0"
            else: # base_ts == last_ts
                return f"{base_ts}-{last_seq + 1}"
        
        return stream_id
    
    def xadd(self, key, stream_id, fields_dict):
        with self.lock:
            # Validate and generate ID
            if stream_id == "*":
                new_id = self._generate_id(key, f"{int(time.time() * 1000)}-*")
            elif stream_id.endswith("-*"):
                new_id = self._generate_id(key, stream_id)
            else:
                new_id = stream_id

            ts, seq = self._parse_id(new_id)
            if ts is None or seq is None or ts < 0 or seq < 0 or (ts == 0 and seq == 0):
                raise ValueError("The ID specified in XADD must be greater than 0-0")

            if not self._validate_id(key, new_id):
                raise ValueError("The ID specified in XADD is equal or smaller than the target stream top item")
            
            if key not in self.data:
                self.data[key] = []

            entry = {"id": new_id, "fields": fields_dict}
            self.data[key].append(entry)

            return new_id
        
    def xrange(self, key, start, end):
        with self.lock:
            if key not in self.data: return []
            stream = self.data[key]
            if start == "-": start = stream[0]["id"]
            if end == "+": end = stream[-1]["id"]
            return [
                entry for entry in stream 
                if self._compare_ids(start, entry["id"]) <= 0 and self._compare_ids(entry["id"], end) <= 0
            ]
    
    def xread(self, streams_to_read):
        with self.lock:
            results = []
            for key, start_id in streams_to_read.items():
                if key not in self.data: continue
                entries = [
                    entry for entry in self.data[key]
                    if self._compare_ids(entry["id"], start_id) > 0
                ]
                if entries:
                    results.append((key, entries))
            return results

    def get_last_id(self, key):
        with self.lock:
            if key in self.data and self.data[key]:
                return self.data[key][-1]["id"]
            return "0-0"
        
    def exists(self, key):
        with self.lock:
            return key in self.data