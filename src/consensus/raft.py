"""Skeleton Raft implementation entrypoints.

This file is a placeholder and will be extended with a full Raft
consensus implementation for the Distributed Lock Manager.
"""
import asyncio
import logging
import random
import time
from typing import List, Any, Dict

logger = logging.getLogger(__name__)


class NotLeaderError(RuntimeError):
    pass


class RaftNode:
    """A minimal, local-simulation Raft node used for development and tests."""

    def __init__(self, node_id: str, peers: List[Any], election_timeout_range=(1.5, 3.0)):
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.log = []  # list of (term, command)
        self.commit_index = -1
        self.leader_id = None
        self._running = False
        self.state = 'follower'
        self._election_task: asyncio.Task = None
        self._election_timeout_range = election_timeout_range
        self._votes_received: Dict[str, bool] = {}
        self._last_heartbeat = time.time()
        self._heartbeat_task: asyncio.Task = None
        self._heartbeat_interval = 0.5

    async def _persist(self):
        """Save persistent state to stable storage before responding to RPCs."""
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log
        }
        try:
            from src.utils.persistence import get_persistence
            # Memastikan sinkronisasi state ke backend (Redis/InMemory)
            await get_persistence().save_raft_state(self.node_id, data)
        except Exception:
            logger.exception("RaftNode %s: Failed to persist state", self.node_id)

    async def start(self):
        self._running = True
        try:
            from src.utils.persistence import get_persistence
            state = await get_persistence().load_raft_state(self.node_id)
            if state:
                self.current_term = state.get('current_term', 0)
                self.voted_for = state.get('voted_for')
                self.log = state.get('log', [])
                logger.info("RaftNode %s: State recovered", self.node_id)
        except Exception:
            logger.error("RaftNode %s: Failed to recover state", self.node_id)

        loop = asyncio.get_event_loop()
        self._election_task = loop.create_task(self._election_loop())

    async def stop(self):
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    async def _election_loop(self):
        try:
            while self._running:
                timeout = random.uniform(*self._election_timeout_range)
                await asyncio.sleep(timeout)
                if not self.is_leader():
                    now = time.time()
                    if now - self._last_heartbeat > timeout:
                        await self.start_election()
        except asyncio.CancelledError:
            return

    async def start_election(self):
        self.current_term += 1
        self.state = 'candidate'
        self.voted_for = self.node_id
        self._votes_received = {self.node_id: True}
        
        await self._persist() 
        
        total = 1 + len(self.peers)
        majority = total // 2 + 1
        tasks = []

        for p in list(self.peers):
            last_idx = len(self.log) - 1
            last_term = self.log[-1][0] if self.log else 0
            
            if hasattr(p, 'on_request_vote'):
                # Local simulation (Unit Test)
                tasks.append(p.on_request_vote(self.current_term, self.node_id, last_idx, last_term))
            else:
                # Remote HTTP (Integration)
                from src.communication.http_transport import post_json
                url = p.rstrip('/') + '/raft/request_vote'
                payload = {'term': self.current_term, 'candidate_id': self.node_id, 
                           'last_log_index': last_idx, 'last_log_term': last_term}
                tasks.append(post_json(url, payload))

        if tasks:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for resp in responses:
                if resp is True or (isinstance(resp, dict) and resp.get('vote_granted')):
                    self._votes_received[getattr(resp, 'node_id', 'remote')] = True

        if len(self._votes_received) >= majority:
            await self.become_leader()

    async def become_leader(self):
        self.leader_id = self.node_id
        self.state = 'leader'
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        try:
            while self._running and self.is_leader():
                await self.replicate_to_peers([])
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            return

    async def append_entry(self, command: Any):
        """Leader entry point to append and replicate logs."""
        if not self.is_leader():
            raise NotLeaderError("node is not the leader")

        entry = (self.current_term, command)
        self.log.append(entry)
        await self._persist()
        
        index = len(self.log) - 1
        # REVISI: Tunggu replikasi selesai sebelum mengembalikan index
        await self.replicate_to_peers(entry)
        
        self.commit_index = index
        return index

    async def replicate_to_peers(self, entry_or_entries: Any):
        """Send logs/heartbeats to all known peers."""
        entries = entry_or_entries if isinstance(entry_or_entries, list) else [entry_or_entries]
        tasks = []

        for p in list(self.peers):
            if hasattr(p, 'receive_append_entries'):
                # Local peer (Unit Test)
                tasks.append(p.receive_append_entries(entries, self.node_id, self.current_term))
            else:
                # Remote peer (Integration)
                from src.communication.http_transport import post_json
                url = p.rstrip('/') + '/raft/append_entries'
                tasks.append(post_json(url, {'entries': entries, 'leader_id': self.node_id, 'term': self.current_term}))
        
        if tasks:
            # Jalankan semua pengiriman secara paralel dan tunggu hasilnya
            await asyncio.gather(*tasks, return_exceptions=True)

    async def receive_append_entries(self, entries: List[Any], leader_id: str, term: int):
        """Follower/Candidate receiving logs from leader."""
        if term >= self.current_term:
            self.current_term = term
            self.leader_id = leader_id
            self.state = 'follower'
            
            if entries:
                for e in entries:
                    if e not in self.log: # Sederhana: hindari duplikasi log
                        self.log.append(e)
                await self._persist()

            self.commit_index = len(self.log) - 1
            self._last_heartbeat = time.time()
            return True
        return False

    async def on_request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> bool:
        """RPC handler for voting requests."""
        if term < self.current_term:
            return False

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

        my_last_term = self.log[-1][0] if self.log else 0
        my_last_index = len(self.log) - 1
        
        up_to_date = (last_log_term > my_last_term) or \
                     (last_log_term == my_last_term and last_log_index >= my_last_index)

        if (self.voted_for is None or self.voted_for == candidate_id) and up_to_date:
            self.voted_for = candidate_id
            await self._persist()
            return True
        return False