from collections import OrderedDict
import asyncio
import aiohttp

class CacheNode:
    def __init__(self, node_id, peers, capacity=5):
        self.node_id = node_id
        self.peers = peers
        self.capacity = capacity

        self.cache = OrderedDict()   # key -> value
        self.state = {}              # key -> MESI state

    # ---------- BASIC ----------
    def get(self, key):
        if key in self.cache and self.state.get(key) != "I":
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    async def put(self, key, value):
        # set ke Modified
        self.cache[key] = value
        self.state[key] = "M"
        self.cache.move_to_end(key)

        # LRU eviction
        if len(self.cache) > self.capacity:
            old_key, _ = self.cache.popitem(last=False)
            del self.state[old_key]

        # broadcast invalidation ke peer
        await self.broadcast_invalidate(key)

    # ---------- COHERENCE ----------
    async def broadcast_invalidate(self, key):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for peer in self.peers:
                url = peer.rstrip("/") + "/cache/invalidate"
                tasks.append(session.post(url, json={"key": key}))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def invalidate(self, key):
        if key in self.cache:
            self.state[key] = "I"

    async def update(self, key, value):
        self.cache[key] = value
        self.state[key] = "S"