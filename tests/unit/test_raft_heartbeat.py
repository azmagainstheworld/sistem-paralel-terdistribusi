import asyncio
import pytest

from src.consensus.raft import RaftNode


@pytest.mark.asyncio
async def test_leader_sends_heartbeats_and_stays_leader():
    a = RaftNode('a', [])
    b = RaftNode('b', [])
    c = RaftNode('c', [])

    a.peers = [b, c]
    b.peers = [a, c]
    c.peers = [a, b]

    await a.start()
    await b.start()
    await c.start()

    # wait for an election to complete
    await asyncio.sleep(0.5)
    leaders = [n.node_id for n in (a, b, c) if n.is_leader()]
    assert len(leaders) == 1
    leader = leaders[0]

    # wait longer than multiple election timeouts to ensure heartbeats keep leader
    await asyncio.sleep(1.0)
    leaders2 = [n.node_id for n in (a, b, c) if n.is_leader()]
    assert leaders2 == [leader]

    await a.stop()
    await b.stop()
    await c.stop()
