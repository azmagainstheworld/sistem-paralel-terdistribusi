"""Simple aiohttp-based HTTP transport for inter-node RPC.

Provides `post_json` to send JSON payloads and a small helper to build
peer URLs. Used by `RaftNode` to replicate log entries to remote peers.
"""
import asyncio
import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


async def post_json(url: str, payload: Any, timeout: float = 5.0) -> Any:
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, json=payload, timeout=timeout) as resp:
                text = await resp.text()
                try:
                    return await resp.json()
                except Exception:
                    return {'status': 'ok', 'raw': text}
    except Exception as e:
        logger.debug('post_json to %s failed: %s', url, e)
        raise
