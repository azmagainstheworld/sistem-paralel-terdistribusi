import os
import asyncio
from fastapi import FastAPI, HTTPException, Body, Request
from pydantic import BaseModel
from typing import Optional, Any

# --- Imports ---
from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager
from src.queue.queue_node import QueueNode
from src.nodes.cache_node import CacheNode

app = FastAPI(title="Distributed Sync System - ITK Project")

# --- Environment ---
node_id = os.getenv("NODE_ID", "node1")
node_port = int(os.getenv("NODE_PORT", 8000))
redis_url = os.getenv("REDIS_URL", "redis://dsync-redis:6379/0")
peers = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []

# --- Initialize Components ---
raft = RaftNode(node_id, peers)
lock_manager = DistributedLockManager(node_id, raft=raft)
base_url = f"http://{node_id}:{node_port}"
queue_node = QueueNode(node_id, base_url=base_url, nodes=peers, redis_url=redis_url)
cache_node = CacheNode(node_id, peers)

# --- Startup ---
@app.on_event("startup")
async def startup_event():
    await raft.start()

    if hasattr(queue_node, "start_recovery_loop"):
        queue_node.start_recovery_loop(lease_seconds=30, interval=10)

    print(f"Node {node_id} running on port {node_port}")

# --- Models ---
class LockRequest(BaseModel):
    resource_name: str
    lock_type: str
    owner_id: str
    ttl: Optional[int] = 30


class QueueRequest(BaseModel):
    queue_name: str
    payload: Any

# =========================
# 🧠 RAFT ENDPOINTS (WAJIB)
# =========================

@app.post("/raft/request_vote")
async def request_vote(req: Request):
    data = await req.json()

    vote = await raft.on_request_vote(
        term=data["term"],
        candidate_id=data["candidate_id"],
        last_log_index=data.get("last_log_index", -1),
        last_log_term=data.get("last_log_term", 0)
    )

    return {"vote_granted": vote}


@app.post("/raft/append_entries")
async def append_entries(req: Request):
    data = await req.json()

    success = await raft.receive_append_entries(
        entries=data.get("entries", []),
        leader_id=data["leader_id"],
        term=data["term"]
    )

    return {"success": success}

# =========================
# 🔐 LOCK ENDPOINTS
# =========================
@app.post("/lock/acquire")
async def acquire_lock(req: LockRequest):
    if not raft.is_leader():
        raise HTTPException(
            status_code=403,
            detail=f"Bukan leader. Leader saat ini: {raft.leader_id}"
        )
    return await lock_manager.acquire_lock(
        req.resource_name,
        req.lock_type,
        req.owner_id,
        req.ttl
    )


# =========================
# 📦 QUEUE ENDPOINTS
# =========================
@app.post("/queue/enqueue")
async def enqueue(req: QueueRequest):
    return await queue_node.publish(req.queue_name, req.payload)


@app.post("/queue/dequeue")
async def dequeue(queue_name: str, consumer_id: str):
    return await queue_node.consume(queue_name, consumer_id)


# =========================
# ⚡ CACHE ENDPOINTS
# =========================
@app.get("/cache/{key}")
async def get_cache(key: str):
    value = cache_node.get(key)
    return {
        "node": node_id,
        "key": key,
        "value": value
    }


@app.post("/cache")
async def put_cache(data: dict = Body(...)):
    key = data.get("key")
    value = data.get("value")

    if key is None:
        raise HTTPException(status_code=400, detail="key is required")

    await cache_node.put(key, value)

    return {
        "node": node_id,
        "status": "updated",
        "key": key
    }


@app.post("/cache/invalidate")
async def invalidate_cache(data: dict = Body(...)):
    key = data.get("key")

    if key is None:
        raise HTTPException(status_code=400, detail="key is required")

    await cache_node.invalidate(key)

    return {
        "node": node_id,
        "status": "invalidated",
        "key": key
    }


# =========================
# ❤️ HEALTH CHECK
# =========================
@app.get("/health")
async def health():
    return {
        "node": node_id,
        "leader": raft.is_leader(),
        "leader_id": raft.leader_id,
        "state": raft.state
    }