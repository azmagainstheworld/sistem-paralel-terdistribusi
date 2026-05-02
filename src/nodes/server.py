"""HTTP server for running a node exposing Raft and Lock manager endpoints.

Run with environment variables:
  NODE_ID (required)
  NODE_HOST (optional, default 0.0.0.0)
  NODE_PORT (optional, default 8000)
  PEERS (optional, comma-separated base URLs for peers, e.g. http://host:8001)
"""
import asyncio
import logging
import os
from typing import List

from aiohttp import web

from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager

logger = logging.getLogger(__name__)


def make_app(raft: RaftNode, lock_manager: DistributedLockManager):
    app = web.Application()

    async def append_entries(request):
        data = await request.json()
        entries = data.get('entries', [])
        leader_id = data.get('leader_id')
        term = data.get('term')
        try:
            await raft.receive_append_entries(entries, leader_id=leader_id, term=term)
            return web.json_response({'status': 'ok'})
        except Exception as e:
            logger.exception('append_entries failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    async def request_vote(request):
        data = await request.json()
        term = data.get('term')
        candidate_id = data.get('candidate_id')
        last_log_index = data.get('last_log_index')
        last_log_term = data.get('last_log_term')
        try:
            # use on_request_vote if local object
            granted = False
            if hasattr(raft, 'on_request_vote'):
                granted = await raft.on_request_vote(term=term, candidate_id=candidate_id, last_log_index=last_log_index, last_log_term=last_log_term)
            return web.json_response({'vote_granted': bool(granted), 'node_id': raft.node_id})
        except Exception as e:
            logger.exception('request_vote failed')
            return web.json_response({'vote_granted': False, 'error': str(e)}, status=500)

    async def lock_acquire(request):
        data = await request.json()
        resource = data['resource']
        mode = data.get('mode', 'exclusive')
        owner = data['owner']
        timeout = data.get('timeout')
        try:
            if not raft.is_leader():
                return web.json_response({'status': 'not_leader', 'leader': raft.leader_id}, status=503)

            await lock_manager.acquire_lock(resource, mode, owner, timeout=timeout)
            return web.json_response({'status': 'acquired'})
        except Exception as e:
            logger.exception('lock_acquire failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    async def lock_release(request):
        data = await request.json()
        resource = data['resource']
        owner = data['owner']
        try:
            if not raft.is_leader():
                return web.json_response({'status': 'not_leader', 'leader': raft.leader_id}, status=503)
            await lock_manager.release_lock(resource, owner)
            return web.json_response({'status': 'released'})
        except Exception as e:
            logger.exception('lock_release failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    # Queue endpoints
    async def queue_publish(request):
        data = await request.json()
        queue = data.get('queue', 'default')
        payload = data.get('message')
        # optional key used for consistent hashing in higher-level dispatch
        key = data.get('key', queue)
        try:
            qnode = request.app['queue_node']
            owner = qnode.owner_for_key(key)
            if owner != qnode.base_url:
                # forward to owner
                from src.communication.http_transport import post_json
                url = owner.rstrip('/') + '/queue/publish'
                resp = await post_json(url, data)
                return web.json_response(resp)

            mid = await qnode.publish(queue, payload)
            return web.json_response({'status': 'published', 'id': mid})
        except Exception as e:
            logger.exception('queue_publish failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    async def queue_consume(request):
        data = await request.json()
        queue = data.get('queue', 'default')
        consumer = data.get('consumer', 'anon')
        timeout = data.get('timeout', 0)
        try:
            qnode = request.app['queue_node']
            # route based on queue name
            owner = qnode.owner_for_key(queue)
            if owner != qnode.base_url:
                from src.communication.http_transport import post_json
                url = owner.rstrip('/') + '/queue/consume'
                resp = await post_json(url, data)
                return web.json_response(resp)

            res = await qnode.consume(queue, consumer, timeout=timeout)
            if not res:
                return web.json_response({'status': 'empty'})
            mid, payload = res
            return web.json_response({'status': 'message', 'id': mid, 'message': payload})
        except Exception as e:
            logger.exception('queue_consume failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    async def queue_ack(request):
        data = await request.json()
        queue = data.get('queue', 'default')
        consumer = data.get('consumer', 'anon')
        mid = data.get('id')
        try:
            qnode = request.app['queue_node']
            owner = qnode.owner_for_key(queue)
            if owner != qnode.base_url:
                from src.communication.http_transport import post_json
                url = owner.rstrip('/') + '/queue/ack'
                resp = await post_json(url, data)
                return web.json_response(resp)

            ok = await qnode.ack(queue, consumer, mid)
            return web.json_response({'status': 'acked' if ok else 'not_found'})
        except Exception as e:
            logger.exception('queue_ack failed')
            return web.json_response({'status': 'error', 'error': str(e)}, status=500)

    async def health(request):
        return web.json_response({'status': 'ok', 'node': raft.node_id, 'leader': raft.leader_id})

    app.router.add_post('/raft/append_entries', append_entries)
    app.router.add_post('/raft/request_vote', request_vote)
    app.router.add_post('/queue/publish', queue_publish)
    app.router.add_post('/queue/consume', queue_consume)
    app.router.add_post('/queue/ack', queue_ack)
    app.router.add_post('/lock/acquire', lock_acquire)
    app.router.add_post('/lock/release', lock_release)
    app.router.add_get('/health', health)

    return app


async def run():
    node_id = os.getenv('NODE_ID')
    if not node_id:
        raise RuntimeError('NODE_ID env var required')

    host = os.getenv('NODE_HOST', '0.0.0.0')
    port = int(os.getenv('NODE_PORT', '8000'))
    peers_env = os.getenv('PEERS', '')
    peers: List[str] = [p.strip() for p in peers_env.split(',') if p.strip()]

    # create raft node with peers (remote peers as URLs)
    raft = RaftNode(node_id, peers)
    lm = DistributedLockManager(node_id, raft=raft)

    await raft.start()
    await lm.start()

    app = make_app(raft, lm)
# --- Di dalam fungsi run() ---
# Cari bagian ini (sekitar baris 179-183):
    
    # attach queue node to app
    try:
        from src.queue.queue_node import QueueNode
        # Pastikan base_url menggunakan host kontainer yang bisa diakses peer lain
        # Di Docker, host seringkali lebih baik menggunakan 0.0.0.0 untuk binding, 
        # tapi base_url harus menggunakan NODE_ID atau IP internal
        base_url = f'http://{node_id}:{port}' 
        nodes_list = list(peers) + [base_url]
        qnode = QueueNode(node_id, base_url=base_url, nodes=nodes_list)
        app['queue_node'] = qnode
    except ImportError as e:
        logger.error(f"Gagal mengimpor QueueNode: {e}")
        raise

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    logger.info('Node %s running on %s:%s peers=%s', node_id, host, port, peers)

    # start queue recovery loop (requeue stale pending messages)
    lease_seconds = int(os.getenv('QUEUE_LEASE_SECONDS', '30'))
    recovery_interval = int(os.getenv('QUEUE_RECOVERY_INTERVAL', '10'))
    try:
        qnode.start_recovery_loop(lease_seconds=lease_seconds, interval=recovery_interval)
    except Exception:
        logger.exception('failed to start recovery loop')

    # run forever
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            qnode.stop_recovery_loop()
        except Exception:
            logger.exception('failed to stop qnode recovery loop')
        await lm.stop()
        await raft.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
