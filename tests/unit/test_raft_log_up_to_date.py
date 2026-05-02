import pytest
import asyncio
from src.consensus.raft import RaftNode
from src.utils.persistence import InMemoryPersistence

@pytest.mark.asyncio
async def test_request_vote_denied_if_candidate_log_older():
    """Mengetes penolakan vote jika log kandidat tertinggal[cite: 10]."""
    follower = RaftNode('f', [])
    follower.log = [(1, {}), (2, {'cmd': 'x'})]
    follower.current_term = 2
    # Injeksi persistence agar await _persist() di dalam raft bekerja[cite: 10, 20].
    follower.persistence = InMemoryPersistence()

    # Case: Term kandidat sama, tapi log index kandidat (0) < log index follower (1).
    granted = await follower.on_request_vote(
        term=2, 
        candidate_id='c', 
        last_log_index=0, 
        last_log_term=1
    )
    assert granted is False

@pytest.mark.asyncio
async def test_request_vote_granted_if_candidate_log_up_to_date():
    """Mengetes pemberian vote jika log kandidat sudah paling mutakhir[cite: 10]."""
    follower = RaftNode('f', [])
    follower.log = [(1, {}), (2, {'cmd': 'x'})]
    follower.current_term = 2
    follower.persistence = InMemoryPersistence()

    # Case: Term kandidat lebih tinggi (3 > 2) dan log setara/lebih baru.
    # Harus menggunakan await karena on_request_vote adalah coroutine[cite: 10, 20].
    granted = await follower.on_request_vote(
        term=3, 
        candidate_id='c', 
        last_log_index=2, 
        last_log_term=3
    )
    assert granted is True