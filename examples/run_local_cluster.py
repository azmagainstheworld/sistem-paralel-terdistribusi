"""Run a 3-node local cluster in a single process (HTTP-based).

PERBAIKAN: 
1. Menambahkan mekanisme pelepasan kunci (release) secara eksplisit.
2. Memperbaiki penanganan siklus hidup (lifecycle) untuk mencegah 'coroutine never awaited'.
3. Menambahkan jeda antar operasi agar replikasi Raft selesai[cite: 10, 32].
"""
import asyncio
import logging
import time

import aiohttp
from aiohttp import web

from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager
from src.nodes.server import make_app

# Set logging ke level INFO untuk melihat proses pemilihan leader
logging.basicConfig(level=logging.INFO)

async def start_node(node_id: str, port: int, peers: list):
    """Menginisialisasi dan menjalankan satu node kluster[cite: 8, 33]."""
    raft = RaftNode(node_id, peers)
    lm = DistributedLockManager(node_id, raft=raft)
    
    # Memulai state Raft dan Lock Manager[cite: 7, 10]
    await raft.start()
    await lm.start()
    
    app = make_app(raft, lm)
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Menggunakan host 127.0.0.1 agar bisa diakses oleh ClientSession lokal[cite: 13, 31]
    site = web.TCPSite(runner, host='127.0.0.1', port=port)
    await site.start()
    
    return {'id': node_id, 'port': port, 'raft': raft, 'lm': lm, 'runner': runner}

async def main():
    base_ports = [8001, 8002, 8003]
    base_urls = [f'http://127.0.0.1:{p}' for p in base_ports]

    nodes = []
    for i, port in enumerate(base_ports):
        # Peer adalah URL dari node lain dalam kluster[cite: 8, 33]
        peers = [u for j, u in enumerate(base_urls) if j != i]
        node = await start_node(f'node{i+1}', port, peers)
        nodes.append(node)

    print('Cluster started. Waiting for leader election...')

    leader = None
    async with aiohttp.ClientSession() as sess:
        # Polling untuk menemukan node yang menjadi leader[cite: 10, 33]
        for _ in range(30):
            for n in nodes:
                try:
                    async with sess.get(f'http://127.0.0.1:{n["port"]}/health', timeout=1) as resp:
                        j = await resp.json()
                        if j.get('leader') == j.get('node'):
                            leader = n
                            break
                except Exception:
                    pass
            if leader:
                break
            await asyncio.sleep(0.5) # Memberi waktu pemilihan suara[cite: 10]

        if not leader:
            print('No leader elected; shutting down kluster.')
            await shutdown(nodes)
            return

        print(f"Leader elected: {leader['id']} on port {leader['port']}")

        resource_name = 'integration-res'
        
        # 1. ACQUIRE LOCK: Owner 'tester' mengambil kunci eksklusif[cite: 7, 8]
        acquire_payload = {'resource': resource_name, 'mode': 'exclusive', 'owner': 'tester'}
        async with sess.post(f"http://127.0.0.1:{leader['port']}/lock/acquire", json=acquire_payload, timeout=5) as r:
            print('acquire response:', await r.json())
        
        # Jeda singkat agar log 'acquire' tereplikasi ke semua node[cite: 10]
        await asyncio.sleep(0.5)

        # 2. CONFLICT CHECK: Owner 'tester2' mencoba mengambil kunci yang sama
        # Menambahkan 'timeout' agar tidak menggantung jika kunci gagal didapat[cite: 7, 34]
        conflict_payload = {'resource': resource_name, 'mode': 'exclusive', 'owner': 'tester2', 'timeout': 1}
        async with sess.post(f"http://127.0.0.1:{leader['port']}/lock/acquire", json=conflict_payload, timeout=5) as r2:
            res_data = await r2.json()
            print('second acquire response (conflict expected):', res_data)

        # 3. RELEASE LOCK: Melepaskan kunci agar tidak memicu deteksi deadlock pada tes selanjutnya[cite: 7, 32]
        release_payload = {'resource': resource_name, 'owner': 'tester'}
        async with sess.post(f"http://127.0.0.1:{leader['port']}/lock/release", json=release_payload, timeout=5) as r3:
            print('release response:', await r3.json())

    # shutdown kluster secara bersih
    await shutdown(nodes)

async def shutdown(nodes):
    """Menghentikan semua komponen kluster untuk membersihkan loop asinkronus[cite: 8, 32]."""
    print('Shutting down kluster...')
    for n in nodes:
        try:
            # Menghentikan background tasks Raft dan Lock Manager[cite: 7, 10]
            await n['lm'].stop()
            await n['raft'].stop()
            await n['runner'].cleanup()
        except Exception as e:
            print(f"Error during shutdown of {n['id']}: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass