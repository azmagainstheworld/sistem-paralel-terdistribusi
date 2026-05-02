import asyncio
import time
import sys
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

# ensure repo root is on sys.path for imports like `src...`
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.nodes.server import make_app
from src.queue.queue_node import QueueNode

# --- MOCK CLASSES ---
class DummyRaft:
    def __init__(self, node_id):
        self.node_id = node_id
        self.leader_id = node_id

    def is_leader(self):
        return True

    def on_request_vote(self, *args, **kwargs):
        return False

    def receive_append_entries(self, *args, **kwargs):
        return None

class DummyLockManager:
    async def start(self): return None
    async def stop(self): return None
    async def acquire_lock(self, *a, **k): return None
    async def release_lock(self, *a, **k): return None

# --- HELPER ---
async def _start_node(port, node_id, peers):
    raft = DummyRaft(node_id)
    lm = DummyLockManager()
    app = make_app(raft, lm)
    base_url = f'http://127.0.0.1:{port}'
    nodes = list(peers) + [base_url]
    
    # In-memory mode for integration testing efficiency
    qnode = QueueNode(node_id, base_url=base_url, nodes=nodes)
    app['queue_node'] = qnode

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', port)
    await site.start()
    return runner, qnode

# --- INTEGRATION TEST ---
@pytest.mark.asyncio
async def test_queue_routing_and_recovery():
    """
    Test flow:
    1. Publish to Node 1.
    2. Consume from Node 2 (forwarding check).
    3. Forget to ACK.
    4. Wait for lease expiry.
    5. Re-consume from Node 1 (recovery check).
    """
    ports = [9001, 9002, 9003]
    runners = []
    qnodes = []
    peers = [f'http://127.0.0.1:{p}' for p in ports]
    
    try:
        # 1. Start cluster nodes
        for i, p in enumerate(ports):
            other_peers = [x for x in peers if x != f'http://127.0.0.1:{p}']
            runner, qn = await _start_node(p, f'node{i+1}', other_peers)
            runners.append(runner)
            qnodes.append(qn)

        # 2. Start recovery loops with AGGRESSIVE timing for test speed
        lease_sec = 1
        for qn in qnodes:
            qn.start_recovery_loop(lease_seconds=lease_sec, interval=1)

        await asyncio.sleep(0.2)

        async with aiohttp.ClientSession() as sess:
            # 3. PUBLISH: Target Node 9001
            publish_payload = {'queue': 'q_test', 'message': {'v': 'top_secret'}, 'key': 'k1'}
            async with sess.post('http://127.0.0.1:9001/queue/publish', json=publish_payload) as resp:
                assert resp.status == 200
                j = await resp.json()
                assert j.get('status') == 'published'
                original_mid = j.get('id')

            # 4. CONSUME: Request from Node 9002 (checks forwarding to the actual owner)
            await asyncio.sleep(5.0)  # <--- TAMBAHKAN BARIS INI
            cpayload = {'queue': 'q_test', 'consumer': 'c1', 'timeout': 5}
            async with sess.post('http://127.0.0.1:9002/queue/consume', json=cpayload) as resp2:
                assert resp2.status == 200
                j2 = await resp2.json()
                assert j2.get('status') == 'message'
                assert j2.get('id') == original_mid
                assert j2.get('message')['v'] == 'top_secret'

            # 5. RECOVERY: Do not ACK. Wait for lease (1s) + buffer.
            # Message should be moved back to available queue.
            await asyncio.sleep(8.0)

            # 6. RE-CONSUME: Try again from any node (e.g. Node 9003)
            await asyncio.sleep(12.0) # <--- PERLAMA JEDA RECOVERY
            async with sess.post('http://127.0.0.1:9003/queue/consume', json=cpayload) as resp3:
                assert resp3.status == 200
                j3 = await resp3.json()
                # If recovery worked, message is back
                assert j3.get('status') == 'message'
                assert j3.get('id') == original_mid
                
                # 7. ACK: Now properly acknowledge to remove it forever
                ack_payload = {'queue': 'q_test', 'consumer': 'c1', 'id': original_mid}
                async with sess.post('http://127.0.0.1:9003/queue/ack', json=ack_payload) as resp4:
                    assert resp4.status == 200
                    j4 = await resp4.json()
                    assert j4.get('status') == 'acked'

    finally:
        # Stop all background loops to avoid hangs or port-in-use errors
        for qn in qnodes:
            qn.stop_recovery_loop()
        for r in runners:
            await r.cleanup()