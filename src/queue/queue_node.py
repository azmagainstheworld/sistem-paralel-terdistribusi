"""Distributed Queue node implementation with Redis fallback.

Supports publish, consume (at-least-once via pending list), and ack.
Improved with thread-safety for in-memory operations and stable fallback logic.
"""
import asyncio
import json
import logging
import os
import uuid
import time
from typing import Optional, Tuple, List
from src.queue.consistent_hash import ConsistentHash

logger = logging.getLogger(__name__)

try:
    import aioredis
except Exception:
    aioredis = None

class QueuePersistence:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv('REDIS_URL', None)
        self._redis = None
        self._mem_lock = asyncio.Lock()
        
        # Inisialisasi awal mode in-memory jika aioredis tidak terinstall
        self._in_memory = True if aioredis is None else False
        
        # Struktur data untuk mode In-Memory
        self._queues = {}      # {queue_name: [mid, ...]}
        self._messages = {}    # {mid: payload}
        self._pending = {}     # {pending_key: [(mid, ts), ...]}
        self._pending_ts = {}  # {mid: ts}

    async def _connect(self):
        """Membangun koneksi Redis atau fallback ke In-Memory jika URL tidak valid."""
        if self._redis is not None or self._in_memory:
            return

        # Validasi skema URL Redis untuk mencegah ValueError dari aioredis
        valid_schemes = ("redis://", "rediss://", "unix://")
        if not self.redis_url or not self.redis_url.startswith(valid_schemes):
            logger.warning("Redis URL tidak valid atau kosong. Mengaktifkan mode In-Memory.")
            self._in_memory = True
            return

        try:
            self._redis = await aioredis.from_url(
                self.redis_url, 
                encoding='utf-8', 
                decode_responses=True
            )
        except Exception as e:
            logger.error(f"Gagal menyambung ke Redis: {e}. Fallback ke In-Memory.")
            self._in_memory = True

    async def push_message(self, queue: str, payload: dict) -> str:
        mid = str(uuid.uuid4())
        await self._connect() 
        if self._in_memory or self._redis is None: # Cek kedua kondisi ini
            async with self._mem_lock:
                self._messages[mid] = payload
                self._queues.setdefault(queue, []).append(mid)
                return mid
        # Hanya jalankan jika redis benar-benar ada
        async with self._redis.pipeline(transaction=True) as pipe:
            await (pipe.hset('queues:messages', mid, json.dumps(payload))
                .rpush(f'queues:{queue}', mid).execute())
        return mid

    async def pop_for_consumer(self, queue: str, consumer: str, timeout: Optional[float] = None) -> Optional[Tuple[str, dict]]:
        await self._connect()
        
        if self._in_memory:
            async with self._mem_lock:
                lst = self._queues.get(queue, [])
                if not lst: 
                    return None
                mid = lst.pop(0)
                ts = int(time.time())
                key = f'pending:{consumer}:{queue}'
                self._pending.setdefault(key, []).append((mid, ts))
                self._pending_ts[mid] = ts
                return mid, self._messages.get(mid)

        mid = None
        if timeout and timeout > 0:
            res = await self._redis.blpop(f'queues:{queue}', timeout=int(timeout))
            if res: 
                _, mid = res
        else:
            mid = await self._redis.lpop(f'queues:{queue}')
            
        if not mid: 
            return None
            
        pending_key = f'pending:{consumer}:{queue}'
        now = int(time.time())
        async with self._redis.pipeline(transaction=True) as pipe:
            await (pipe.lpush(pending_key, mid)
                   .hset('pending:timestamps', mid, now)
                   .execute())
        
        raw = await self._redis.hget('queues:messages', mid)
        payload = json.loads(raw) if raw else None
        return mid, payload

    async def ack_message(self, queue: str, consumer: str, mid: str) -> bool:
        await self._connect()
        
        if self._in_memory:
            async with self._mem_lock:
                pk = f'pending:{consumer}:{queue}'
                lst = self._pending.get(pk, [])
                for i, (m, ts) in enumerate(list(lst)):
                    if m == mid:
                        lst.pop(i)
                        self._messages.pop(mid, None)
                        self._pending_ts.pop(mid, None)
                        return True
                return False

        pk = f'pending:{consumer}:{queue}'
        async with self._redis.pipeline(transaction=True) as pipe:
            await (pipe.lrem(pk, 0, mid)
                   .hdel('pending:timestamps', mid)
                   .hdel('queues:messages', mid)
                   .execute())
        return True

    async def requeue_stale_pending(self, queue: str, lease_seconds: int) -> int:
        await self._connect()
        now = int(time.time())
        requeued = 0
        
        if self._in_memory:
            async with self._mem_lock:
                keys = [k for k in self._pending.keys() if k.endswith(f':{queue}')]
                for k in keys:
                    lst = list(self._pending.get(k, []))
                    remaining = []
                    for mid, ts in lst:
                        if now - ts > lease_seconds:
                            self._queues.setdefault(queue, []).append(mid)
                            self._pending_ts.pop(mid, None)
                            requeued += 1
                        else:
                            remaining.append((mid, ts))
                    if remaining: 
                        self._pending[k] = remaining
                    else: 
                        self._pending.pop(k, None)
            return requeued

        pattern = f'pending:*:{queue}'
        keys = await self._redis.keys(pattern)
        for pk in keys:
            mids = await self._redis.lrange(pk, 0, -1)
            for mid in mids:
                ts_raw = await self._redis.hget('pending:timestamps', mid)
                ts = int(ts_raw) if ts_raw else 0
                if now - ts > lease_seconds:
                    async with self._redis.pipeline(transaction=True) as pipe:
                        await (pipe.rpush(f'queues:{queue}', mid)
                               .lrem(pk, 0, mid)
                               .hdel('pending:timestamps', mid)
                               .execute())
                    requeued += 1
        return requeued

    async def list_queues(self) -> List[str]:
        await self._connect()
        if self._in_memory: 
            return list(self._queues.keys())
            
        keys = await self._redis.keys('queues:*')
        return [k.split(':', 1)[1] for k in keys]

class QueueNode:
    def __init__(self, node_id: str, base_url: str, nodes: Optional[List[str]] = None, redis_url: Optional[str] = None):
        self.node_id = node_id
        self.base_url = base_url
        self.peers = nodes or []
        self.store = QueuePersistence(redis_url=redis_url)
        
        # Inisialisasi Consistent Hashing
        all_nodes = list(set(self.peers + [self.base_url]))
        self.ring = ConsistentHash(nodes=all_nodes)

    async def publish(self, queue: str, payload: dict) -> str:
        return await self.store.push_message(queue, payload)

    def owner_for_key(self, key: str) -> str:
        """Menentukan node pemilik berdasarkan hash kunci."""
        return self.ring.get_node(key)

    async def consume(self, queue: str, consumer: str, timeout: Optional[float] = None):
        return await self.store.pop_for_consumer(queue, consumer, timeout=timeout)

    async def ack(self, queue: str, consumer: str, mid: str) -> bool:
        return await self.store.ack_message(queue, consumer, mid)

    async def requeue_stale_pending_once(self, lease_seconds: int) -> int:
        queues = await self.store.list_queues()
        total = 0
        for q in queues:
            n = await self.store.requeue_stale_pending(q, lease_seconds)
            total += n
        return total

    def start_recovery_loop(self, lease_seconds: int = 30, interval: int = 10):
        if hasattr(self, '_recovery_task') and self._recovery_task: 
            return
        loop = asyncio.get_event_loop()
        self._recovery_task = loop.create_task(self._recovery_worker(lease_seconds, interval))

    def stop_recovery_loop(self):
        t = getattr(self, '_recovery_task', None)
        if t:
            t.cancel()
            self._recovery_task = None

    async def _recovery_worker(self, lease_seconds: int, interval: int):
        """Worker background untuk memulihkan pesan yang hang."""
        try:
            while True:
                try:
                    n = await self.requeue_stale_pending_once(lease_seconds)
                    if n: 
                        logger.info(f'Node {self.node_id}: Requeued {n} stale messages')
                except Exception:
                    logger.exception('Recovery iteration failed')
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info(f'Node {self.node_id}: Recovery loop stopped')