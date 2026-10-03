# Redis Streams: a practical one-page guide

A **Redis Stream** is an ordered log of entries. Each entry has an ID
and fields such as `event_id`, `type`, and `payload`. Producers append
with `XADD`; readers fetch new entries with `XREAD` or revisit a range
with `XRANGE`. Unlike Redis Pub/Sub, entries remain available until
deleted or trimmed. Persistence and retention settings determine how
long they survive.

## Reading and processing

- **Independent readers** (`XREAD`)
  - Each reader keeps its own last-seen ID and can see every entry.
  - Examples: chat participants, dashboards, event replay.
- **Consumer group** (`XREADGROUP`)
  - Workers in the same group divide new entries. Redis tracks
    delivered but unacknowledged entries.
  - Examples: job queues and event-processing worker pools.

A group belongs to one stream. Workers acknowledge successful
processing with `XACK`. If one crashes, another can inspect pending
work (`XPENDING`) and reclaim idle entries (`XAUTOCLAIM`). Workers on
many VMs can join the same group, each with a distinct consumer
name. One worker can also read several streams, provided the group
exists on each.

**Delivery is at least once.** A worker might finish and crash before
`XACK`, so an entry can run again. The worker code consuming the
stream must make its business action idempotent. One way is to record
a stable event ID under a database unique constraint in the same
transaction as the business update. For external APIs, use their
idempotency keys. Acknowledge after safely recording the
result. `XACK` does not delete the entry: once old entries are no
longer needed for recovery or replay, trim them with `XTRIM` or `XADD
MAXLEN`.

## How common uses fit

- **Changing worker pool**
  - Natural fit: VMs join one consumer group as demand rises and leave
    as it falls.
  - Extra design: Drain work on shutdown; reclaim abandoned work after
    a delay longer than normal processing time.
- **Many quiet sources (1,000+ streams)**
  - Natural fit: One worker can read multiple streams.
  - Extra design: Discovering streams and managing groups and pending
    work adds overhead. A shared stream with a `source_id` field is
    simpler when sources have the same rules.
- **Job queue**
  - Natural fit: `XADD` submits; `XREADGROUP` assigns; `XACK`
    completes.
  - Extra design: Build retries, dead-letter handling, status, and
    cleanup. Delays and priorities need extra structures.
- **Group chat**
  - Natural fit: A stream per channel provides ordered history, live
    reads, and reconnect catch-up.
  - Extra design: Users need independent cursors: a consumer group
    splits messages instead of broadcasting. Edits, reactions, search,
    and long-lived history need more storage or state.
- **Monitoring**
  - Natural fit: `XRANGE` reads history without consuming it;
    `XPENDING` shows unacknowledged work.
  - Extra design: After `XACK`, the group keeps no completion
    flag. Store application status separately.

## Example: Jobs that create more jobs

For a parent job that spawns descendants, use **two kinds of
streams**:

- A **shared work stream (work queue)** holds jobs ready for the
  worker group to process.
- A **stream per root job (workflow log)** records `workflow_started`,
  `job_queued`, `job_finished`, and the final result. A dashboard can
  replay or follow this stream independently.

Include the root in `job_queued`, with a unique job ID for every
job. Each job emits exactly one terminal event (`job_finished` or
`job_failed`) after announcing all its direct children. The workflow
ends when every queued job ID has a terminal event. Under a
fail-if-any-child-fails policy, any `job_failed` makes the final
result a failure. A child being retried remains unfinished.

Registering a child, adding it to the work queue, and appending its
`job_queued` event must be **atomic** (for example, in a Redis
script). Retries must not append duplicate queued or terminal
events. A dashboard can replay the workflow log to determine its
status. To emit one `workflow_completed` or `workflow_failed` event, a
coordinator can replay the log and use an atomic marker so the final
event is appended once, even after a crash.
