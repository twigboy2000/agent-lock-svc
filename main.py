"""
agent-lock-svc — FastAPI GPU lock service
==========================================
Manages the agent:gpu:lock Redis key as a proper HTTP service.
All scripts call this instead of touching Redis directly.

Endpoints:
  GET  /health              → 200 OK
  GET  /lock/status         → lock state (locked, item_id, age_s, ttl_s)
  POST /lock/acquire        → acquire lock (409 if busy)
  POST /lock/release        → release lock (validates ownership)
  POST /lock/force-release  → admin clear, no ownership check
"""

import json
import os
import time
from typing import Optional

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASS = os.environ.get("REDIS_PASS", "")
LOCK_KEY   = "agent:gpu:lock"
LOCK_TTL   = int(os.environ.get("LOCK_TTL", "7200"))  # 2h hard cap

app = FastAPI(title="agent-lock-svc", version="1.0.0")


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASS or None,
        db=0,
        socket_timeout=5,
        decode_responses=True,
    )


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class AcquireRequest(BaseModel):
    item_id: str            # e.g. "pr-review:twigboy2000/trade-system-ml:29"
    item_type: str          # pr-review | qa-run | revision-required | story-pickup
    repo: str               # e.g. "twigboy2000/trade-system-ml"
    ttl: Optional[int] = None  # override default TTL


class ReleaseRequest(BaseModel):
    item_id: Optional[str] = None   # if provided, validates ownership before release
    status: Optional[str] = "ok"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_lock(r: redis.Redis) -> Optional[dict]:
    raw = r.get(LOCK_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw, "item_id": "unreadable", "acquired_at": 0}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    r = get_redis()
    r.ping()
    return {"status": "ok"}


@app.get("/lock/status")
def lock_status():
    r = get_redis()
    lock = read_lock(r)
    if not lock:
        return {"locked": False}

    ttl_s = r.ttl(LOCK_KEY)
    age_s = int(time.time()) - lock.get("acquired_at", int(time.time()))
    return {
        "locked":    True,
        "item_id":   lock.get("item_id"),
        "item_type": lock.get("item_type"),
        "repo":      lock.get("repo"),
        "acquired_at": lock.get("acquired_at"),
        "age_s":     age_s,
        "ttl_s":     ttl_s,
    }


@app.post("/lock/acquire")
def lock_acquire(req: AcquireRequest):
    r = get_redis()

    lock_value = json.dumps({
        "item_id":     req.item_id,
        "item_type":   req.item_type,
        "repo":        req.repo,
        "acquired_at": int(time.time()),
    })
    ttl = req.ttl or LOCK_TTL
    result = r.set(LOCK_KEY, lock_value, nx=True, ex=ttl)

    if result is not True:
        # Lock already held — return current holder for caller context
        lock = read_lock(r)
        ttl_s = r.ttl(LOCK_KEY)
        age_s = int(time.time()) - (lock or {}).get("acquired_at", int(time.time()))
        raise HTTPException(status_code=409, detail={
            "error":     "GPU lock already held",
            "holder":    (lock or {}).get("item_id"),
            "age_s":     age_s,
            "ttl_s":     ttl_s,
        })

    return {"acquired": True, "item_id": req.item_id, "ttl": ttl}


@app.post("/lock/release")
def lock_release(req: ReleaseRequest):
    r = get_redis()
    lock = read_lock(r)

    if not lock:
        return {"released": False, "reason": "no lock present"}

    # Ownership check
    if req.item_id and lock.get("item_id") != req.item_id:
        raise HTTPException(status_code=403, detail={
            "error":    "lock belongs to a different item",
            "holder":   lock.get("item_id"),
            "requester": req.item_id,
        })

    age_s = int(time.time()) - lock.get("acquired_at", int(time.time()))
    r.delete(LOCK_KEY)
    return {
        "released": True,
        "item_id":  lock.get("item_id"),
        "age_s":    age_s,
        "status":   req.status,
    }


@app.post("/lock/force-release")
def lock_force_release():
    """Admin endpoint — no ownership check. Used by watchdog."""
    r = get_redis()
    lock = read_lock(r)

    if not lock:
        return {"released": False, "reason": "no lock present"}

    age_s = int(time.time()) - lock.get("acquired_at", int(time.time()))
    r.delete(LOCK_KEY)
    return {
        "released":  True,
        "item_id":   lock.get("item_id"),
        "age_s":     age_s,
        "forced":    True,
    }
