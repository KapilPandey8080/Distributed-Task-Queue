from dataclasses import dataclass
from functools import lru_cache

import redis
from redis.exceptions import ResponseError

from app.config import get_settings

GROUP = "workers"


@dataclass(frozen=True)
class Delivery:
    """One message handed to a worker. Must be acknowledged when finished."""

    entry_id: str  # Redis's id for this stream entry, e.g. "1729400000000-0"
    job_id: str  # our Postgres job id
    queue: str


@lru_cache
def get_redis() -> redis.Redis:
    return redis.Redis.from_url(get_settings().redis_url, decode_responses=True)


def stream_key(queue: str) -> str:
    return f"tq:stream:{queue}"


class RedisQueue:
    """Thin wrapper over a Redis Stream + consumer group.

    Redis only carries job ids. Postgres stays the source of truth for job state.
    """

    def __init__(self, client: redis.Redis | None = None) -> None:
        self.r = client or get_redis()
        self._groups_ready: set[str] = set()

    def _ensure_group(self, key: str) -> None:
        if key in self._groups_ready:
            return
        try:
            # id="0" means a brand-new group still receives entries added before it existed.
            self.r.xgroup_create(key, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # group already exists: fine
                raise
        self._groups_ready.add(key)

    def enqueue(self, queue: str, job_id: str) -> str:
        key = stream_key(queue)
        self._ensure_group(key)
        return self.r.xadd(key, {"job_id": str(job_id)})

    def read(self, queue: str, consumer: str, block_ms: int = 5000) -> Delivery | None:
        """Wait up to block_ms for the next job. Returns None on timeout."""
        key = stream_key(queue)
        self._ensure_group(key)
        # ">" = only entries never delivered to any consumer in this group.
        response = self.r.xreadgroup(GROUP, consumer, {key: ">"}, count=1, block=block_ms)
        if not response:
            return None
        _stream, entries = response[0]
        entry_id, fields = entries[0]
        return Delivery(entry_id=entry_id, job_id=fields["job_id"], queue=queue)

    def ack(self, delivery: Delivery) -> None:
        """Tell Redis this entry is done, then delete it so the stream doesn't grow forever."""
        key = stream_key(delivery.queue)
        self.r.xack(key, GROUP, delivery.entry_id)
        self.r.xdel(key, delivery.entry_id)