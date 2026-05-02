import asyncio
import pytest

from src.consensus.raft import RaftNode


@pytest.mark.asyncio
async def test_local_election_three_nodes():
    # create three nodes as objects and wire peers as object refs
    a = RaftNode('a', [])
    b = RaftNode('b', [])
    c = RaftNode('c', [])

    a.peers = [b, c]
    b.peers = [a, c]
    c.peers = [a, b]

    await a.start()
    await b.start()
    await c.start()

    # give election loop time to run and elect a leader
    await asyncio.sleep(0.5)

    leaders = [n.node_id for n in (a, b, c) if n.is_leader()]
    # exactly one leader expected
    assert len(leaders) == 1

    # cleanup
    await a.stop()
    await b.stop()
    await c.stop()
