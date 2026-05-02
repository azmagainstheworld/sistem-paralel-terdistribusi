"""Simple consistent hashing ring for partitioning queues across nodes."""
import hashlib
from bisect import bisect_right
from typing import List, Dict


class ConsistentHash:
    def __init__(self, nodes: List[str] = None, replicas: int = 100):
        self.replicas = replicas
        self.ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        if nodes:
            for n in nodes:
                self.add_node(n)

    def _hash(self, key: str) -> int:
        h = hashlib.md5(key.encode('utf-8')).hexdigest()
        return int(h, 16)

    def add_node(self, node: str):
        for i in range(self.replicas):
            v = f"{node}#{i}"
            k = self._hash(v)
            self.ring[k] = node
            self._sorted_keys.append(k)
        self._sorted_keys.sort()

    def remove_node(self, node: str):
        to_remove = [k for k, v in self.ring.items() if v == node]
        for k in to_remove:
            del self.ring[k]
            self._sorted_keys.remove(k)

    def get_node(self, key: str) -> str:
        if not self.ring:
            raise KeyError('no nodes')
        k = self._hash(key)
        idx = bisect_right(self._sorted_keys, k)
        if idx == len(self._sorted_keys):
            idx = 0
        return self.ring[self._sorted_keys[idx]]
