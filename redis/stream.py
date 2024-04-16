import asyncio,  re, time
from collections import defaultdict
from dataclasses import dataclass
import redis.resp as resp
from app.utils import to_pairs

class StreamTrigger():
    def __init__(self, array):
        self.event = asyncio.Event()
        self.conditions = list(to_pairs(array))

@dataclass
class Stream:
    store = defaultdict(list) # type: ignore

    def add(self, key: str, entry) -> bytes:
        id = entry[0]
        entries = self.store[key]
        if id == '0-0':
            return resp.simple_error('The ID specified in XADD must be greater than 0-0')
        
        last_entry_id = entries[-1][0] if len(entries) else None
        if last_entry_id: last_ts, last_seq = last_entry_id.split('-')

        if match := re.search(r'(\d*)-\*', id):
            ts = match.group(1)
            if not last_entry_id:
                id = f'{ts}-{int(ts == "0")}'
            else:
                seq = 0 if ts > last_ts else int(last_seq) + 1
                id = f'{ts}-{seq}'
        elif id == '*':
            if not last_entry_id:
                ts = int(time.time() * 1000)
                id = f'{ts}-0'
            else:
                id = f'{last_ts}-{int(last_seq) + 1}'
        elif last_entry_id and id <= last_entry_id:
            return resp.simple_error('The ID specified in XADD is equal or smaller than the target stream top item')

        entry[0] = id
        entries.append(entry)
        return resp.bulk_string(id)
        
    def read(self, key: str):
        return self.store.get(key)

    def range(self, key: str, start: str, end: str) -> bytes:
        def test(entry):
            return (start == '-' or start <= entry[0]) and (end == '+' or entry[0] <= end)

        entries = self.store.get(key)
        return list(filter(test, entries)) # type: ignore
