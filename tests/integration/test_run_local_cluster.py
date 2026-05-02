import asyncio
import socket
import pytest

# Mengimpor modul skrip utama untuk dijalankan dalam tes integrasi[cite: 14, 30]
from examples import run_local_cluster

def _ports_available(ports):
    """Mengecek apakah port 8001-8003 tersedia untuk menghindari konflik binding[cite: 14, 30]."""
    for p in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', p))
            except OSError:
                return False
    return True

@pytest.mark.asyncio
async def test_run_local_cluster_completes():
    # 1. Validasi lingkungan tes[cite: 14, 30]
    ports = [8001, 8002, 8003]
    if not _ports_available(ports):
        pytest.skip('Port 8001-8003 sedang digunakan, melewatkan tes integrasi[cite: 14, 30].')

    try:
        # 2. Eksekusi kluster simulasi dengan batas waktu 20 detik[cite: 14, 30]
        # Jika run_local_cluster.main() memicu DeadlockDetected, exception akan ditangkap di sini.
        await asyncio.wait_for(run_local_cluster.main(), timeout=20)
        
    except asyncio.TimeoutError:
        pytest.fail("Tes gagal: Timeout 20 detik tercapai (kemungkinan ada task menggantung atau deadlocked)[cite: 14].")
    except Exception as e:
        # Menangani exception spesifik DeadlockDetected dari LockManager[cite: 7, 8]
        if "Deadlock detected" in str(e):
            pytest.fail(f"Integrasi Gagal: Logika LockManager mendeteksi deadlock pada skrip simulasi: {e}")
        else:
            pytest.fail(f"Terjadi error tidak terduga saat eksekusi kluster: {e}")

    finally:
        # 3. Cleanup: Menghentikan semua task sisa (seperti ClientSession atau Loop background)[cite: 8, 11]
        # Ini mencegah 'coroutine was never awaited' di akhir sesi pytest[cite: 8, 15].
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        # Menunggu task dibatalkan secara aman tanpa melempar exception ke pytest[cite: 15, 22]
        await asyncio.gather(*tasks, return_exceptions=True)