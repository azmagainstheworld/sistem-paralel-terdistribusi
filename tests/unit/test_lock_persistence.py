# tests/unit/test_lock_persistence.py
import asyncio
import pytest
from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager
from src.utils.persistence import InMemoryPersistence # Pastikan ini diimpor

@pytest.mark.asyncio
async def test_lock_persistence_inmemory():
    # Gunakan instance terisolasi total
    shared_p = InMemoryPersistence()
    node = RaftNode('a', [])
    node.persistence = shared_p # Bypass get_persistence()
    
    await node.start_election()
    await asyncio.sleep(0.5)
    
    lm1 = DistributedLockManager('a', raft=node)
    lm1.persistence = shared_p 
    await lm1.start()
    
    await lm1.acquire_lock('resX', 'exclusive', 'ownerX')
    await asyncio.sleep(0.2) # Jeda untuk simulasi IO asinkronus

    lm2 = DistributedLockManager('a-recover', raft=node)
    lm2.persistence = shared_p 
    await lm2.start()

    assert 'resX' in lm2.lock_table