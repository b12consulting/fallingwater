import hashlib
import bisect
import time
import uuid
import redis


# =====================================================================
# 1. CONSISTENT HASH RING IMPLEMENTATION
# =====================================================================
class ConsistentHashRing:
    def __init__(self, nodes: list[str] = None, vnodes: int = 100):
        self.vnodes = (
            vnodes  # Virtual nodes per worker to ensure even distribution
        )
        self.ring = {}  # Maps virtual node hash -> worker_id
        self.sorted_keys = []  # Sorted list of hashes

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node_id: str):
        for i in range(self.vnodes):
            vnode_key = f"{node_id}#vnode_{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node_id
            bisect.insort(self.sorted_keys, h)

    def get_node(self, stream_id: str) -> str | None:
        if not self.ring:
            return None
        h = self._hash(stream_id)
        idx = bisect.bisect_right(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0  # Wrap around the ring
        return self.ring[self.sorted_keys[idx]]


# =====================================================================
# 2. HASH-RING WORKER NODE
# =====================================================================
class StreamWorker:
    def __init__(
        self, redis_client: redis.Redis, group_name: str, worker_id: str = None
    ):
        self.r = redis_client
        self.group = group_name
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:6]}"

        self.REGISTRY_KEY = "cluster:workers:active"
        self.HEARTBEAT_TTL = 6  # Seconds before considering a worker dead
        self.MIN_IDLE_TIME_MS = 10000  # 10s idle threshold for XAUTOCLAIM

    def _heartbeat_and_discover_nodes(self) -> list[str]:
        """
        Registers worker heartbeat and fetches all currently active workers.
        """
        now = time.time()
        cutoff = now - self.HEARTBEAT_TTL

        # 1. Update own heartbeat
        self.r.zadd(self.REGISTRY_KEY, {self.worker_id: now})

        # 2. Prune dead workers from ZSET registry
        self.r.zremrangebyscore(self.REGISTRY_KEY, "-inf", cutoff)

        # 3. Fetch list of currently active worker IDs
        active_workers = self.r.zrangebyscore(self.REGISTRY_KEY, cutoff, "+inf")
        return [w for w in active_workers]

    def _ensure_group_exists(self, stream_id: str):
        try:
            self.r.xgroup_create(stream_id, self.group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def process_message(self, stream_id: str, msg_id: str, data: dict):
        print(f"[{self.worker_id}] Executing event from '{stream_id}': {data}")
        time.sleep(0.1)  # Simulate processing workload

    def run_cycle(self, target_streams: list[str]):
        # Step 1: Discover active workers and build local Hash Ring
        active_nodes = self._heartbeat_and_discover_nodes()
        ring = ConsistentHashRing(nodes=active_nodes)

        # Step 2: Identify streams owned by THIS worker
        owned_streams = [
            stream_id
            for stream_id in target_streams
            if ring.get_node(stream_id) == self.worker_id
        ]

        if not owned_streams:
            return

        for stream_id in owned_streams:
            self._ensure_group_exists(stream_id)

            # Step 3: HANDOVER / RECOVERY (Claim abandoned pending messages from dead nodes)
            try:
                claimed = self.r.xautoclaim(
                    name=stream_id,
                    groupname=self.group,
                    consumername=self.worker_id,
                    min_idle_time=self.MIN_IDLE_TIME_MS,
                    start_id="0-0",
                    count=1,
                )
                if (
                    claimed and claimed[1]
                ):  # If an abandoned message was claimed
                    msg_id, data = claimed[1][0]
                    self.process_message(stream_id, msg_id, data)
                    self.r.xack(stream_id, self.group, msg_id)
                    continue  # Move to next task after recovery
            except redis.exceptions.ResponseError:
                pass  # Pre-Redis 6.2 compatibility

            # Step 4: STRICT 1-BY-1 CONSUMPTION (Fetch next single message)
            entries = self.r.xreadgroup(
                groupname=self.group,
                consumername=self.worker_id,
                streams={stream_id: ">"},
                count=1,
                block=100,  # Non-blocking wait across assigned streams
            )

            if entries and entries[0][1]:
                msg_id, data = entries[0][1][0]
                self.process_message(stream_id, msg_id, data)
                self.r.xack(stream_id, self.group, msg_id)


# =====================================================================
# 3. DEMONSTRATION / EXECUTION LOOP
# =====================================================================
if __name__ == "__main__":
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # List of 5 independent inbox stream keys
    ALL_STREAMS = [f"stream:inbox:{i}" for i in range(1, 6)]

    # Seed 1 event into each inbox stream
    for s in ALL_STREAMS:
        r.xadd(s, {"task": f"Process inbox payload for {s}"})

    # Instantiate two distinct workers running on the hash ring
    worker_A = StreamWorker(
        r, group_name="inbox_processors", worker_id="node_Alpha"
    )
    worker_B = StreamWorker(
        r, group_name="inbox_processors", worker_id="node_Beta"
    )

    print("--- Executing Worker Cycles ---")
    # Both workers evaluate the same target streams list
    worker_A.run_cycle(ALL_STREAMS)
    worker_B.run_cycle(ALL_STREAMS)
