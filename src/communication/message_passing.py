"""Basic message passing utilities (placeholder).

Will be expanded to support aiohttp/gRPC transports between nodes.
"""
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

async def send_message(addr: str, payload: dict):
    logger.debug("send_message to %s: %s", addr, payload)
    # placeholder: real implementation will use aiohttp or gRPC
    await asyncio.sleep(0.01)
    return {"status": "ok"}
