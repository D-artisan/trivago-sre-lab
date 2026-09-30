import os

import redis
from fastapi import FastAPI, HTTPException

REDIS_HOST = os.getenv("REDIS_HOST", "redis.lab.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

app = FastAPI(title="Hotel Materialized View API")


@app.get("/health")
def health():
    r.ping()
    return {"status": "ok"}


@app.get("/hotels/{hotel_id}")
def get_hotel(hotel_id: str):
    data = r.hgetall(f"hotel:{hotel_id}")
    if not data:
        raise HTTPException(status_code=404, detail="hotel not found")
    return {"hotel_id": hotel_id, **data}
