import asyncio
import logging
from typing import Dict, Optional

from src.nodes.base_node import Node
from src.consensus.raft import RaftNode, NotLeaderError
from src.utils.persistence import get_persistence

logger = logging.getLogger(__name__)

class LockUnavailable(Exception):
    pass

class DeadlockDetected(Exception):
    pass

class DistributedLockManager(Node):
    """
    Distributed lock manager menggunakan Raft consensus.
    Mendukung Shared/Exclusive locks dan deteksi deadlock.
    """

    def __init__(self, node_id: str, raft: RaftNode):
        super().__init__(node_id)
        self.raft = raft
        # resource -> {'mode': 'exclusive'/'shared', 'owners': set()}
        self.lock_table: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        # resource -> list of {'owner', 'mode', 'future'}
        self.waiters: Dict[str, list] = {}

    def _add_waiter(self, resource: str, owner: str, mode: str) -> asyncio.Future:
        fut = asyncio.get_event_loop().create_future()
        self.waiters.setdefault(resource, []).append({
            'owner': owner, 
            'mode': mode, 
            'future': fut
        })
        return fut

    def _remove_waiter(self, resource: str, owner: str):
        lst = self.waiters.get(resource, [])
        self.waiters[resource] = [w for w in lst if w['owner'] != owner]
        if not self.waiters[resource]:
            self.waiters.pop(resource, None)

    async def start(self):
        await super().start()
        self.persistence = get_persistence()
        try:
            persisted = await self.persistence.load_lock_table()
            if persisted:
                # Pastikan 'owners' dikonversi kembali menjadi set() jika dari JSON
                for res in persisted:
                    if 'owners' in persisted[res]:
                        persisted[res]['owners'] = set(persisted[res]['owners'])
                self.lock_table = persisted
                logger.info("LockManager %s: Table recovered", self.node_id)
        except Exception:
            logger.exception("failed to load persisted lock table")

    def apply_log_entry(self, command: dict):
        """Menerapkan entri log Raft ke tabel kunci lokal."""
        op = command.get('op')
        resource = command.get('resource')
        mode = command.get('mode')
        owner = command.get('owner')

        lt = self.lock_table.setdefault(resource, {'mode': None, 'owners': set()})

        if op == 'acquire':
            lt['mode'] = mode
            lt['owners'].add(owner)
        elif op == 'release':
            lt['owners'].discard(owner)
            if not lt['owners']:
                lt['mode'] = None
        if hasattr(self, 'persistence') and self.persistence:
                try:
                    # Gunakan get_running_loop untuk memastikan loop masih aktif
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        loop.create_task(self.persistence.save_lock_table(self.lock_table))
                except RuntimeError:
                    # Abaikan jika loop sudah ditutup (terjadi saat teardown unit test)
                    pass

    async def acquire_lock(self, resource: str, mode: str, owner: str, timeout: Optional[float] = None):
        """Acquire lock dengan konsensus Raft dan deteksi deadlock."""
        async with self._lock:
            if not self.raft.is_leader():
                raise NotLeaderError('not leader')

            lt = self.lock_table.get(resource, {'mode': None, 'owners': set()})
            
            # Cek kompatibilitas kunci
            can_acquire = False
            if lt['mode'] is None:
                can_acquire = True
            elif lt['mode'] == 'shared' and mode == 'shared':
                can_acquire = True

            if can_acquire:
                cmd = {'op': 'acquire', 'resource': resource, 'mode': mode, 'owner': owner}
                await self.raft.append_entry(cmd)
                self.apply_log_entry(cmd)
                return

            # Jika tidak bisa langsung dapet, antre
            fut = self._add_waiter(resource, owner, mode)

            # --- DETEKSI DEADLOCK ---
            if self._detect_deadlock(new_waiter=(owner, resource)):
                self._remove_waiter(resource, owner)
                # PERBAIKAN: Hapus teks sisa citation di sini
                raise DeadlockDetected(f"Deadlock detected for owner {owner} on {resource}")

        # Tunggu di luar lock utama agar tidak memblokir release_lock node lain
        try:
            await asyncio.wait_for(fut, timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError, DeadlockDetected):
            async with self._lock:
                self._remove_waiter(resource, owner)
            raise 

        # Finalisasi setelah dibangunkan (Wake Up)
        async with self._lock:
            if not self.raft.is_leader():
                raise NotLeaderError('not leader')
            cmd = {'op': 'acquire', 'resource': resource, 'mode': mode, 'owner': owner}
            await self.raft.append_entry(cmd)
            self.apply_log_entry(cmd)

    async def release_lock(self, resource: str, owner: str):
        """Melepaskan kunci dan membangunkan antrean berikutnya."""
        async with self._lock:
            if not self.raft.is_leader():
                raise NotLeaderError('not leader')

            cmd = {'op': 'release', 'resource': resource, 'owner': owner}
            await self.raft.append_entry(cmd)
            self.apply_log_entry(cmd)
            
            await self._try_wake_waiters(resource)

    async def _try_wake_waiters(self, resource: str):
        """Logika FIFO untuk membangunkan waiter yang kompatibel."""
        lst = self.waiters.get(resource, [])
        if not lst: return

        lt = self.lock_table.get(resource, {'mode': None, 'owners': set()})
        if lt['mode'] is not None: return

        if lst[0]['mode'] == 'shared':
            to_wake = []
            for w in list(lst):
                if w['mode'] == 'shared': to_wake.append(w)
                else: break
            for w in to_wake:
                if not w['future'].done():
                    w['future'].set_result(True)
                self._remove_waiter(resource, w['owner'])
        
        elif lst[0]['mode'] == 'exclusive':
            head = lst[0]
            if not head['future'].done():
                head['future'].set_result(True)
            self._remove_waiter(resource, head['owner'])

    def _detect_deadlock(self, new_waiter):
        """Deteksi siklus pada Wait-For Graph menggunakan DFS."""
        owner_new, res_new = new_waiter
        wait_for = {}

        for res, lst in self.waiters.items():
            holders = self.lock_table.get(res, {'owners': set()})['owners']
            for w in lst:
                wait_for.setdefault(w['owner'], set()).update(holders)

        # Tambahkan edge baru yang memicu deteksi
        holders_new = self.lock_table.get(res_new, {'owners': set()})['owners']
        wait_for.setdefault(owner_new, set()).update(holders_new)

        visited = set()
        stack = set()

        def dfs(u):
            visited.add(u)
            stack.add(u)
            for v in wait_for.get(u, ()): 
                if v in stack: return True
                if v not in visited:
                    if dfs(v): return True
            stack.remove(u)
            return False

        for node in list(wait_for.keys()):
            if node not in visited:
                if dfs(node): return True
        return False