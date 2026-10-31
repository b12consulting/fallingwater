import redis
import time
import threading
from random import random


r = redis.Redis(host="localhost", port=6379, decode_responses=True)

STREAM = "order_events"
GROUP = "processors"
NB_ITER = 100


def setup():
    r.delete(STREAM)
    try:
        r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)

    # Seed 3 sequential messages
    for i in range(NB_ITER):
        r.xadd(STREAM, {"event": f"{i}x1: Create Account"})
        r.xadd(STREAM, {"event": f"{i}x2: Deposit $100"})
        r.xadd(STREAM, {"event": f"{i}x3: Withdraw $50"})


def worker(consumer_id: str) -> bool:
    # 1. Fetch the next message using '>'
    entries = r.xreadgroup(GROUP, consumer_id, {STREAM: ">"}, count=1)
    if not entries or not entries[0][1]:
        print(f"[{consumer_id}] No new messages.")
        return

    msg_id, data = entries[0][1][0]
    event = data["event"]

    # FLAW 1: DATA EXPOSURE
    # The payload is already in worker memory before XPENDING is checked
    print(f"[{consumer_id}] GOT PAYLOAD OVER NETWORK: '{event}' (ID: {msg_id})")

    # 2. Inspect XPENDING for older unacked messages held by peer consumers
    pending = r.xpending_range(STREAM, GROUP, min="-", max="+", count=10)

    for item in pending:
        p_id = item["message_id"]
        p_consumer = item["consumer"]

        if p_id < msg_id and p_consumer != consumer_id:
            print(
                f"[{consumer_id}] DETECTED earlier pending message {p_id} held by {p_consumer}!"
            )
            print(
                f"[{consumer_id}] ABORTING and issuing XNACK for {msg_id}...\n"
            )

            # Redis 8.8+ XNACK command releases the message back to the group PEL
            r.xnack(STREAM, GROUP, "SILENT", msg_id)
            return False

    # If no older message is pending, process and ACK
    print(f"[{consumer_id}] PROCESSED & ACKED: '{event}'\n")
    r.xack(STREAM, GROUP, msg_id)
    return True


def worker_loop(
    consumer_id: str, iterations: int = NB_ITER, delay: float = 10.0
):
    for _ in range(iterations):
        success = worker(consumer_id)
        if not success:
            time.sleep(random() * delay)


if __name__ == "__main__":
    setup()

    for name in "ABC":
        threading.Thread(target=worker_loop, args=(f"Consumer_{name}",)).start()
