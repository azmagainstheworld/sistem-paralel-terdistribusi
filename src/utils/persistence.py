"""Persistence layer with Redis backend and in-memory fallback.

Provides a simple async API used by the lock manager to persist the Raft log
and the lock table. If Redis is not available, an in-memory fallback is used
so tests and local development continue to work.
"""
import asyncio
import json
import logging
import os
from typing import Any, List, Dict, Optional

logger = logging.getLogger(__name__)

_singleton = None

try:
    import aioredis
except Exception:
    aioredis = None


class BasePersistence:
    async def append_log(self, entry: dict) -> int:
        raise NotImplementedError()

    async def get_log(self) -> List[dict]:
        raise NotImplementedError()

    async def save_lock_table(self, table: Dict[str, Dict]) -> None:
        raise NotImplementedError()

    async def load_lock_table(self) -> Dict[str, Dict]:
        raise NotImplementedError()

    # Metode baru untuk status Raft
    async def save_raft_state(self, node_id: str, data: Dict) -> None:
        raise NotImplementedError()

    async def load_raft_state(self, node_id: str) -> Dict:
        raise NotImplementedError()


class InMemoryPersistence(BasePersistence):
    def __init__(self):
        self._log: List[dict] = []
        self._lock_table: Dict[str, Dict] = {}
        self._raft_state: Dict[str, Dict] = {}  # Store status Raft di memori
        self._lock = asyncio.Lock()

    async def append_log(self, entry: dict) -> int:
        async with self._lock:
            self._log.append(entry)
            return len(self._log) - 1

    async def get_log(self) -> List[dict]:
        async with self._lock:
            return list(self._log)

    async def save_lock_table(self, table: Dict[str, Dict]) -> None:
        async with self._lock:
            self._lock_table = {k: {'mode': v.get('mode'), 'owners': list(v.get('owners', []))} for k, v in table.items()}

    async def load_lock_table(self) -> Dict[str, Dict]:
        async with self._lock:
            return {k: {'mode': v.get('mode'), 'owners': set(v.get('owners', []))} for k, v in self._lock_table.items()}

    # Implementasi status Raft untuk pengujian lokal
    async def save_raft_state(self, node_id: str, data: Dict) -> None:
        async with self._lock:
            self._raft_state[node_id] = data

    async def load_raft_state(self, node_id: str) -> Dict:
        async with self._lock:
            return self._raft_state.get(node_id, {})


class RedisPersistence(BasePersistence):
    def __init__(self, url: str = None):
        if aioredis is None:
            raise RuntimeError('aioredis not available')
        self.url = url or os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self._redis = None

    async def _connect(self):
        if self._redis is None:
            self._redis = await aioredis.from_url(self.url, encoding='utf-8', decode_responses=True)

    async def append_log(self, entry: dict) -> int:
        await self._connect()
        s = json.dumps(entry)
        idx = await self._redis.rpush('raft:log', s)
        return int(idx) - 1

    async def get_log(self) -> List[dict]:
        await self._connect()
        vals = await self._redis.lrange('raft:log', 0, -1)
        return [json.loads(v) for v in vals]

    async def save_lock_table(self, table: Dict[str, Dict]) -> None:
        await self._connect()
        serial = {k: {'mode': v.get('mode'), 'owners': list(v.get('owners', []))} for k, v in table.items()}
        await self._redis.set('locks:table', json.dumps(serial))

    async def load_lock_table(self) -> Dict[str, Dict]:
        await self._connect()
        val = await self._redis.get('locks:table')
        if not val:
            return {}
        serial = json.loads(val)
        return {k: {'mode': v.get('mode'), 'owners': set(v.get('owners', []))} for k, v in serial.items()}

    # Implementasi Redis untuk persistensi status Raft antar restart kontainer
    async def save_raft_state(self, node_id: str, data: Dict) -> None:
        await self._connect()
        key = f"raft:state:{node_id}"
        await self._redis.set(key, json.dumps(data))

    async def load_raft_state(self, node_id: str) -> Dict:
        await self._connect()
        key = f"raft:state:{node_id}"
        val = await self._redis.get(key)
        return json.loads(val) if val else {}


def get_persistence() -> BasePersistence:
    global _singleton
    if _singleton is not None:
        return _singleton

    url = os.getenv('REDIS_URL')
    if url and aioredis is not None:
        try:
            _singleton = RedisPersistence(url=url)
            return _singleton
        except Exception:
            logger.exception('failed to initialize RedisPersistence, falling back to in-memory')

    _singleton = InMemoryPersistence()
    return _singleton