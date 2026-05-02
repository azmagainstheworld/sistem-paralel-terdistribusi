import asyncio
import logging

logger = logging.getLogger(__name__)

class Node:
    """Minimal node skeleton to be extended by lock/queue/cache nodes.

    Provides lifecycle hooks and a simple in-memory state.
    """
    def __init__(self, node_id: str, host: str = "127.0.0.1", port: int = 0):
        self.node_id = node_id
        self.host = host
        self.port = port
        self._running = False
        self._tasks = []

    async def start(self):
        self._running = True
        logger.info("Node %s starting", self.node_id)

    async def stop(self):
        self._running = False
        logger.info("Node %s stopping", self.node_id)
        for t in self._tasks:
            t.cancel()

    def is_running(self) -> bool:
        return self._running
