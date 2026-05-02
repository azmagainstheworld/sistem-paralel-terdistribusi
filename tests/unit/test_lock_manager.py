import asyncio
import pytest

from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager
from src.utils.persistence import InMemoryPersistence

# REVISI: Gunakan dekorator eksplisit untuk fixture asinkronus
@pytest.fixture
async def cleanup_tasks(): # Hapus autouse=True
    yield
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

@pytest.mark.asyncio
async def test_lock_acquire_release_shared_exclusive(cleanup_tasks):
    # 1. Inisialisasi Node & Force In-Memory
    node_a = RaftNode('a', [])
    node_b = RaftNode('b', [])
    node_c = RaftNode('c', [])

    for n in [node_a, node_b, node_c]:
        n.persistence = InMemoryPersistence()

    node_a.peers = [node_b, node_c]
    node_b.peers = [node_a, node_c]
    node_c.peers = [node_a, node_b]

    lm_a = DistributedLockManager('a', raft=node_a)
    lm_a.persistence = InMemoryPersistence()
    await lm_a.start()

    # 2. Pemilihan Leader
    await node_a.start_election()
    await asyncio.sleep(0.5) 
    assert node_a.is_leader()

    # 3. Operasi Lock
    await lm_a.acquire_lock('res1', 'exclusive', 'owner1')

    # Tunggu replikasi log Raft ke peer[cite: 10, 34]
    replicated = False
    for _ in range(20):
        if len(node_b.log) > 0 and len(node_c.log) > 0:
            replicated = True
            break
        await asyncio.sleep(0.1)

    assert replicated
    assert lm_a.lock_table['res1']['mode'] == 'exclusive'

    # 4. Conflict Check menggunakan timeout[cite: 7, 34]
    with pytest.raises(asyncio.TimeoutError):
        await lm_a.acquire_lock('res1', 'exclusive', 'owner2', timeout=0.2)

    # 5. Release Lock
    await lm_a.release_lock('res1', 'owner1')
    await asyncio.sleep(0.2)
    assert lm_a.lock_table.get('res1', {}).get('mode') is None