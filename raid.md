# RAID log

Track technical risks, assumptions, issues, and decisions that depend
on how Fallingwater is implemented. Record the status and reason for
each choice, including when to revisit it. Add a decision when a
technical choice is discussed or an implementation must be
revisited. Close or revise an item when the code resolves it.

## Risks

- **R1 — Duplicate external effects (open).** Workers skip results already
  visible when they load a turn, and `save_tool_result` checks for a
  saved call result before appending another. These reads are not
  atomic with the writes, so concurrent workers can still duplicate
  model runs, results, terminal events, or dispatch actions. A crash
  after a tool's external side effect but before saving its result can
  also run the tool again. Attached tools need their own idempotency
  strategy for effects that must occur once; D72 defines the execution
  identity Fallingwater should provide to support that strategy.
- **R2 — Conversation changes across upgrades (open).** An agent
  definition may change while a conversation is idle. The conversation
  stores its import path but no version, so later turns may run
  different behavior.
- **R3 — Event volume (open).** Each deferred model step and each tool
  result adds a conversation event, while every tool call also adds a
  dispatch entry. Long conversations make turn reconstruction and the
  event scan used for result fan-in increasingly expensive. Measure
  stream size and processing cost before choosing retention,
  snapshots, or incremental reconstruction.
- **R4 — Claimed work can stall (closed).** The worker uses
  `XAUTOCLAIM` to recover dispatch entries after a configurable idle
  period. A pending item still waits until the configured threshold
  elapses.
- **R5 — Active actions can be reclaimed (open).**
  `Worker.claim_idle_ms` defaults to 300,000 ms (5 minutes), while the
  installed Pydantic AI/Ollama HTTP client allows 600 seconds per
  timeout phase and the OpenAI SDK retries timeouts up to twice. A
  dispatch entry can therefore become eligible for `XAUTOCLAIM` while
  its original worker is still running. There is no agent-run or
  tool-call lock, so both model calls and tools may run twice. Set
  explicit action time limits and keep the claim interval longer than
  the bounded action duration, or renew pending-entry idle time during
  processing. Increasing the interval above one HTTP timeout alone
  does not cover retries or long-running tools.
- **R6 — Child results beyond the first page (closed).**
  `ConversationStream.iter_events()` advances through Redis range
  pages with an exclusive entry-ID cursor for turn reconstruction.
  Child-result actions now carry the exact terminal event ID, so the
  parent reads that event directly regardless of stream length.
- **R7 — Unbounded tool or child-agent chains (open).** Each deferred
  step schedules another independent action, and the current worker
  has no limit on tool rounds or child-agent depth. An agent that
  keeps requesting tools or recursively delegates can keep consuming
  model calls and stream space. Decide on explicit per-turn limits
  before running untrusted or costly agents.
- **R8 — Partial cross-stream writes (open).** Conversation events and
  their dispatch actions are now separate Redis commands. A process or
  Redis failure between commands can leave a queued turn, tool
  request, child request, or child terminal event without the action
  needed to continue it. Recovery of the original dispatch action may
  skip saved events and will not necessarily recreate the missing
  action. Add repair or transactional writes before relying on
  unattended recovery.
## Assumptions

- **A1 — Serialized prompts when required (accepted).** A sender that
  needs sequential conversation history waits for one turn to finish
  before submitting the next. Independent senders may submit
  overlapping turns, which can read the same prior history.
- **A2 — Redis is the first transport (to validate).** Other backends
  are outside the first release; assess whether application interfaces
  can stay independent of Redis commands when they are expanded.
## Issues

- **I1 — Conversation ownership (closed).** Workers do not reserve a
  conversation; senders coordinate prompt submission when sequential
  history is required.
- **I2 — Agent catalog (open).** Import-path loading works, including
  `AgentSpec` objects, and `fw ls` lists conversation IDs. File-based
  agent specs, catalog listing of agents, and application-level
  capability composition remain undecided. Define their naming and
  registration before adding them.
- **I3 — Event contract and retention (open).** Decide which progress
  events are durable, how long conversation history remains available,
  and how SSE readers resume from a cursor. The per-conversation feed
  replays from the beginning on every connection; the recent feed
  replays up to ten events per selected conversation. Completion
  deltas must remain available for history replay; creation and queued
  events must remain while workers may need to load them from a poke
  or while a CLI reader group still has them pending.
- **I4 — Conversation ending (open).** Decide what ends a
  conversation, how its status is recorded, and what happens to its
  stream and future submissions.
- **I5 — Dispatch stream retention (open).** `XACK` clears pending
  status but leaves the entry in `fw:dispatch`; decide when
  acknowledged entries are deleted without losing recoverable work.
- **I6 — Redis integration coverage (open).** Unit tests exercise
  sequential result writes and recovery with an in-memory Redis
  implementation, but do not exercise worker restart, pending-entry
  claims, or concurrent workers against a real Redis server. Add
  integration coverage for those paths.
- **I7 — Per-tool-call dispatch (closed).** Implemented one dispatch
  entry per deferred call. Result fan-in compares unique call IDs from
  the persisted request and result events, then separately queues a
  resume when the final result is saved. Tool side effects remain
  at-least-once if a worker stops after the side effect and before its
  result is stored. Child agents are queued in their own conversation
  streams and notify the parent on completion or failure, so a parent
  worker does not wait for child work and the worker pool needs no
  reserved child capacity.
- **I8 — Action-specific agent loading (closed).** `Worker.process`
  routes `turn.subagent.result` before loading the parent agent or
  validating dependencies. Model-run and attached-tool actions still
  load them, while child results use saved stream state even if the
  parent agent import path is unavailable.
- **I9 — Changing a conversation's model (open).** The model is saved
  only in `conversation.created`; `Chat` ignores a new model supplied
  when resuming, and workers reconstruct the saved one. If that model
  is retired or becomes unavailable, later turns may fail and there is
  no supported way to continue the same conversation with another
  model. Decide whether a model-change event sets the default for
  later turns or each queued turn records its model, how to validate
  the replacement, and what happens to queued or in-progress turns
  during a change. Replay must preserve which model was selected for
  each turn rather than reinterpret earlier turns using the latest
  model.
## Decisions

- **D1 — Two stream roles (decided).** Use one shared `fw:dispatch` stream
  for worker dispatch and one event stream per conversation for
  history and observers. The dispatch name leaves room for other
  message types later; workers can discover work through one consumer
  group while readers follow a conversation independently.
- **D2 — Turn boundary (superseded).** A dispatch entry remains
  pending for the logical turn, while the conversation stream
  checkpoints deferred model results and tool outputs. Workers stop
  between saved steps; `XAUTOCLAIM` resumes the same turn.
- **D3 — Completion history as deltas (decided).** Write the
  accumulated messages for the current turn in `turn.completed`:
  deferred-step message deltas followed by the final run's
  `new_messages()`. Replay completed turns in stream order to rebuild
  later history. Delta semantics are implicit; full-conversation
  snapshots are outside the supported event contract. Add an explicit
  format field if snapshots are introduced later.
- **D4 — Conversation IDs (decided).** The CLI generates readable
  `qualifier_scientist` IDs for resumption; library-created
  conversations use UUIDs. Both map to the same per-conversation
  stream naming scheme.
- **D5 — Fixed agent and model (to revisit; see I9).** The creation
  event identifies the agent import path and model for a
  conversation. `Chat` uses those saved values on resume, ignoring
  supplied agent and model options so a conversation cannot change
  them. This preserves current reconstruction behavior, but the model
  policy needs a way to handle unavailable models.
- **D6 — Admission of overlapping turns (decided).** Fallingwater does
  not enforce one active turn per conversation. Senders serialize
  prompts when they need each turn to include the preceding
  result. The `workers` consumer group assigns dispatch entries
  individually, so overlapping turns can run on different workers.
- **D7 — Pending work and duplicate effects (partially decided).**
  Recover abandoned dispatch entries with `XAUTOCLAIM`. A saved
  terminal event prevents a completed turn from running again, and
  saved per-call tool results are skipped. A crash after a tool side
  effect but before saving its result can repeat that call; external
  side effects remain at-least-once.
- **D8 — Dispatch stream cleanup (open).** Choose when to delete
  acknowledged dispatch entries without removing pending entries that
  still need recovery. `XACK` alone leaves the entry in `fw:dispatch`.
- **D9 — In-process conversation cache (open).** Keep reconstructed
  conversation history in Python process memory, together with the
  last conversation stream entry ID read. On the next access, read
  only entries after that ID and apply new completed-turn
  deltas. Rebuild from the stream after a cache miss or worker
  restart; decide how long cached conversations stay in memory.
- **D10 — Conversation ending (open).** Choose how an ended state is
  represented in the event stream, how submissions are rejected
  afterward, and when its data can be removed.
- **D11 — Agent definition versioning (open).** Decide what identity
  or version to store with a turn and how a resumed conversation
  selects an agent definition after an upgrade.
- **D12 — Progress event contract (decided).** Persist each deferred
  tool request with that model call's message delta and usage, then
  persist each tool result individually. Keep final output and
  accumulated message history in `turn.completed`; ordinary chat
  clients ignore intermediate events. Child agents use their own
  conversation streams.
- **D13 — Transport boundary (open).** Decide which library interfaces
  should hide Redis commands while preserving stream semantics for a
  possible later backend.
- **D14 — Catalog composition (open).** Decide how file-based specs
  and application-provided tools and capabilities are registered,
  named, and combined before adding catalog listing commands.
- **D15 — Dispatch payload (decided).** A `conversation.poke`
  instruction carries only a conversation ID and turn ID. Workers load
  the matching `turn.queued` prompt, fixed agent and model, and prior
  history from the conversation stream. Keeping the turn ID identifies
  the queued event even when senders allow overlapping turns; the
  dispatch stream can carry other instruction types later.
- **D16 — Chat validation (decided).** `Chat` validates the selected
  agent and available model before creating or resuming a
  conversation. The CLI passes its agent and model arguments directly
  to `Chat`, which chooses saved settings for an existing
  conversation. CLI-generated names are checked for existing streams
  before use.
- **D17 — Web event feed (decided).** Export a module-level FastAPI
  router from the optional `web` package and mount it under `/fw-api`
  in the small `fw web` app. The hosting app supplies a synchronous
  Redis client and namespace through app state. Use synchronous
  `XREAD` for non-consuming SSE readers; the SSE response advances
  synchronous iterators in a worker thread. The per-conversation feed
  replays from the beginning and uses each stream entry ID as the SSE
  ID. The recent feed selects ten distinct conversations from the
  newest dispatch pokes, reads up to ten retained entries per stream
  with `XREVRANGE`, then follows the streams and dispatch together. It
  prefixes SSE IDs with the conversation ID. Both feeds publish stored
  event types as SSE event names. A packaged Jinja2 template renders
  the read-only monitoring page. Cursor-based reconnection remains
  open in I3.
- **D18 — CLI reader recovery (amended by D60).** Use a consumer group
  on each conversation stream for one logical terminal reader,
  defaulting to `user` and configurable with `--group`. Reuse the
  stable `terminal` consumer name to read pending entries after
  restart. Acknowledge queued and other non-result events as read;
  print completed or failed events before acknowledging them. On
  startup, process available pending and new events without waiting
  for unfinished turns, so a new prompt may overlap an older
  turn. One-shot `--msg` mode uses the same group; independent readers
  use separate groups, while the web feed remains on `XREAD`.
- **D19 — Shared turn message field (decided).** Use `message` for
  both the queued user prompt and the completed agent response; the
  event type identifies its meaning. This gives turn messages one
  consistent field name for producers and readers.
- **D20 — No turn-start event (decided).** Do not append
  `turn.started` to conversation streams. The `workers` consumer
  group's pending dispatch entries expose claimed work while a worker
  processes it; a pending entry may also be left by a crashed worker,
  so it does not prove that work is still active.
- **D21 — Stream access classes (superseded).** The initial
  `RedisStream`, `ReadGroup`, `DispatchStream`, and
  `ConversationStream` wrappers put Redis operation policy in a second
  abstraction layer. They were replaced by direct commands, then a
  shared `ConversationClient`. D44 reintroduces two narrower stream
  roles while workflow policy stays in `Chat` and `Worker`.
- **D22 — Invalid dispatch actions (decided).** If an action is
  malformed or its conversation turn cannot be reconstructed, log and
  acknowledge it without creating a result event because no trusted
  turn state is available. Runtime failures after reconstruction write
  `turn.failed`; Redis read or write errors propagate and leave the
  action pending.
- **D23 — Loaded conversation value (decided).**
  `turn_state.reconstruct_turn` returns a frozen Pydantic
  `Conversation` model containing the agent path, model, next prompt,
  current turn ID, prior history, dependencies, and latest
  usage. Named fields make the loaded work explicit at the processing
  boundary and avoid coupling callers to tuple position; an empty
  history is represented by an empty list.
- **D24 — Stream consumer groups (superseded).** The former
  `ConversationClient` performed group creation, `XREADGROUP`, `XACK`,
  and `XAUTOCLAIM` for `Chat` and `Worker`; D44 divides these
  operations by stream role.
- **D25 — Chat stream access (superseded).** `Chat` formerly delegated
  stream access and turn enqueue to `ConversationClient`; D44 binds it
  to a `ConversationStream`.
- **D26 — Compression benchmark (completed).** Compare Zstandard, LZ4,
  Brotli, and Snappy on a serialized Pydantic AI turn containing two
  messages and separate usage metadata. Measure minimum, medium, and
  maximum levels for codecs that support levels; Snappy has one fixed
  setting. Brotli quality 5 was selected for conversation payloads
  based on its compression ratio and benchmarked cost.
- **D27 — Conversation payload encoding (superseded by D59).** Store
  conversation stream events as `type` and Brotli quality 5 compressed
  Pydantic JSON `payload`; keep dispatch actions text based. Use Redis
  clients with binary responses so payload bytes survive Redis
  reads. Reset existing conversation streams during rollout; legacy
  entry decoding is outside scope. Completed payloads include
  `RunUsage`, which the worker passes into the next sequential run.
- **D28 — Stream naming and codec boundary (decided).** Define
  `fmt_dispatch_stream` and `fmt_conversation_stream` helpers that
  validate their inputs and format the Redis key patterns. Keep the
  payload codec behind `utils.compress` and `utils.decompress`, and
  expose the event payload registry as `PAYLOAD_MODELS`. Stream read
  methods decode returned batches into typed lists;
  `ConversationStream.iter_events` iterates across pages. Use Pydantic
  models for stream events and loaded conversation values.
- **D29 — Separate stream turn IDs and agent run IDs (decided).** Keep
  the `turn_id` in Fallingwater stream events as the application
  correlation key. Let Pydantic AI generate its own `run_id` for each
  `Agent.run_sync` invocation; the stream already carries the
  information Fallingwater needs, and duplicating it into Pydantic AI
  message metadata adds no required behavior.
- **D30 — CLI stream inspection (decided).** Add `fw ls` to scan and
  list conversation IDs, `fw tail <conversation_id>` to decode recent
  conversation events, and `fw dispatch` to decode recent dispatch
  entries. Event commands default to ten entries, accept `-n`, and
  print Redis entry IDs, event types, and compact JSON payloads in
  chronological stream order.
- **D31 — Structured completed history (decided).** Store the
  JSON-compatible list of message objects directly in a completed
  payload's `turn_messages` field (renamed by D75). Serialize the payload once
  at the stream boundary, then validate the message value with
  `ModelMessagesTypeAdapter.validate_python` when loading it. This
  removes the nested JSON string encoding while retaining Pydantic
  AI's message representation.
- **D32 — Namespace character set (decided).** Accept only lowercase
  ASCII letters, digits, and hyphens in Redis namespaces. This keeps
  namespace values suitable as literal Redis key prefixes and prevents
  glob characters from broadening `SCAN MATCH` patterns. Expose the
  validator as `validate_namespace` and call it from both stream-name
  formatters.
- **D33 — Formatted conversation payloads (decided).** Give
  conversation payload models a `fmt(full=False)` method for CLI and
  monitor display. Completed payloads omit `turn_messages` and `usage` by
  default to keep `fw tail` and the monitor readable; `fw tail --full`
  and `/monitor?full=1` include both. The per-conversation SSE API
  always publishes complete payloads.
- **D34 — SSE shutdown (decided).** Emit a comment after idle
  one-second Redis reads so synchronous SSE iterators regularly return
  control. Configure the bundled Uvicorn server with a three-second
  graceful-shutdown timeout to cancel streams that remain open when
  `fw web` stops. Keep the Redis client alive until `uvicorn.run()`
  returns to avoid closing sockets while cancelled stream reads are
  unwinding.
- **D35 — Direct stream operations (superseded).** The direct-command
  implementation in `Chat` and `Worker` exposed duplicated Redis and
  codec work. The CLI inspection commands and web feeds still use
  direct read-only Redis patterns for scans and multi-stream reads.
- **D36 — Deferred attached-tool runner (superseded).** Its in-process
  runner loop is replaced by one-call `agent_proxy.run` and
  stream-driven orchestration in `Worker`; see D38.
- **D37 — Dispatch recovery (decided).** Before reading new dispatch
  entries, each worker advances its in-memory `XAUTOCLAIM` cursor and
  claims entries idle for `claim_idle_ms`. Default to 300,000 ms and
  expose `fw worker --claim-idle-ms`. This recovers entries left
  pending after worker failure; R5 covers active actions that outlast
  the idle interval.
- **D38 — Checkpointed agent execution (superseded).** The earlier
  in-memory dispatch loop executed deferred tool calls sequentially
  and blocked a parent worker while a child `Chat` completed. D40
  replaces it with independent action dispatch, per-call fan-out,
  result fan-in, and nonblocking child conversations.
- **D39 — Shared conversation client (superseded).** The shared
  `ConversationClient` mixed conversation events with dispatch queue
  operations. D44 separates the two stream roles.
- **D40 — Parallel calls and nonblocking child turns (amended by
  D64).** Store each model step's full deferred request set, then
  enqueue one dispatch action per call. Save results by call ID and
  queue an agent-resume action when every requested call has a
  result. A tool can return `SubAgentRequest(agent_path, prompt,
  model?, deps?)`; Fallingwater creates a deterministic child
  conversation, records parent correlation, and queues the child turn
  without blocking the parent worker. Child completion or failure
  queues a parent-result action; child failure fails the parent turn,
  and late sibling results are ignored after terminal failure. There
  is no worker-capacity reservation for child agents. D64 removes the
  original transactions and agent-run lease.
- **D41 — Conversation-scoped client identity (superseded).** The
  optional default conversation ID is replaced by a
  `ConversationStream` bound to exactly one ID; see D44.
- **D42 — Pure turn reconstruction (decided).** Keep Redis pagination
  in `ConversationStream.iter_events` and fold ordered, decoded events
  in `turn_state.reconstruct_turn`. This isolates turn-state rules
  from Redis access; `Worker` calls the pure function with the stream
  iterator.
- **D43 — Worker naming (decided).** Name the Pydantic AI adapter
  `agent_proxy.py`, the dispatch consumer `Worker` in `worker.py`, and
  the thread and signal supervisor `WorkerPool` in
  `worker_pool.py`. The CLI's pool-size option is `-w/--workers`,
  matching the number of dispatch workers it starts.
- **D44 — Split stream access by role (amended by D64).**
  `DispatchStream` owns the shared queue's reads, claims, groups, and
  acknowledgements. It constructs `ConversationStream` for a specific
  conversation ID. The latter handles typed event access and related
  writes to both streams. `Worker` creates a bound stream for each
  dispatch action and child conversation; `Chat` retains one bound
  stream. This removes optional conversation ID resolution and
  prevents conversation operations from accidentally targeting another
  ID. D64 makes related writes separate Redis commands.
- **D45 — Stream module names (superseded by D57).** Put
  `DispatchStream` and `ConversationStream` in `stream.py`, and put
  stream names, event types, payload models, and codecs in
  `stream_utils.py`. The module names distinguished Redis access from
  the definitions it used.
- **D46 — Agent-run lease and tool execution boundaries (superseded by
  D64).** The per-step Redis lease was
  removed. `agent_proxy.execute_tool()` still owns attached-tool
  lookup, argument decoding, basic `RunContext` reconstruction, and
  invocation. `Worker` chooses the action and persists its outcome.
- **D47 — Worker error boundary (decided).** `Worker.run` receives
  decoded dispatch actions, loads their conversations, calls
  `Worker.process`, and decides whether to acknowledge or leave an
  entry pending. It acknowledges malformed or unreconstructable
  actions without a failure event, writes `turn.failed` for non-Redis
  processing errors after loading, and propagates Redis errors for
  recovery. `Worker.process` handles the selected action without a
  surrounding exception handler.
- **D48 — Typed dispatch actions (decided).** `DispatchStream` decodes
  Redis fields into a discriminated action model for each supported
  dispatch type. `Worker` receives validated identifiers and routes by
  action class instead of indexing raw fields. Invalid entries retain
  their stream ID and validation error so `Worker.run` can log and
  acknowledge them under D22. D52 removes the original fallback for a
  missing type.
- **D49 — Stream entry IDs on decoded values (decided).** Attach the
  Redis entry ID to dispatch actions, invalid dispatch actions, and
  conversation events returned by stream
  reads. `DispatchStream.read_group` and the entries in `claim` return
  actions directly; all three `ConversationStream` read methods return
  events directly. The ID is read metadata and is excluded from
  serialized Pydantic payloads. Raw-entry CLI and web inspection
  retain their separate tuple decoder.
- **D50 — Chat turn identity (superseded).** The initial explicit
  turn-ID and idempotency-key options reused queued turns for repeated
  submissions. Removed under D51 because client retry behavior is
  premature at this stage of development.
- **D51 — Fresh turns for client submissions (decided).**
  `Chat.send(prompt)` always generates a new UUID turn ID and queues
  the prompt. `Chat.ask(prompt)` calls `send` and waits for that
  turn's result. Defer explicit client turn IDs, idempotency keys, and
  submission deduplication until a concrete client workflow requires
  their semantics.
- **D52 — Explicit dispatch action types (decided).** Require every
  dispatch entry to include its action type. Missing types produce
  invalid actions, which workers log and acknowledge under D22. Remove
  the implicit `conversation.poke` fallback because compatibility with
  older entries is premature while the stream format is evolving.
- **D53 — Usage scoped to one turn (reverted).** The change to start
  each new turn with fresh usage was reverted after
  reconsideration. Keep the existing usage carryover from previous
  completed turns and restore the latest deferred-step checkpoint when
  resuming. Revisit accounting scope when its intended semantics are
  clearer.
- **D54 — Child failure policy (to revisit).** A failed child
  currently fails its parent turn. Removing that propagation requires
  recording the child failure as a parent tool result so fan-in can
  finish and the parent agent can resume. Decide how failed tool
  results are represented before changing this policy; simply dropping
  the failure branch would leave the parent waiting or cause another
  processing error.
- **D55 — Event and action separation (decided).** Keep
  `conversation.poke` exclusively in `Action`, alongside the other
  dispatch actions. `Event` lists only durable conversation events
  with payload schemas. The short enum names distinguish recorded
  facts from worker commands; their docstrings identify the
  streams. Dispatch writers and monitoring filters use `Action`; the
  monitor can enumerate `Event` without excluding dispatch
  actions. Stored type strings and stream behavior remain unchanged.
- **D56 — Diagram compilation (decided).** Use `make docs` to compile
  every `docs/*.d2` source to an SVG beside its source in `docs/`,
  keeping diagram sources and rendered outputs together. Use source
  timestamps for incremental rendering and invoke `d2` directly
  without configurable executable or rendering flags.
- **D57 — Separate stream schemas from codecs (decided).** Keep event
  and dispatch action enums, payload and action models, their aliases,
  and the payload registry in `stream_models.py`. Keep stream key
  validation, encoding, decoding, and the dispatch type adapter in
  `stream_utils.py`. Callers import schemas directly from the model
  module, making the module boundary explicit without changing stored
  stream entries.
- **D58 — Zstandard archive files (extended by D59).** Write and
  restore conversation archives only as `.jsonl.zst` using Zstandard's
  streaming writer and reader, so archive I/O does not need a manual
  decompressed-line buffer. There is no legacy archive support before
  production.
- **D59 — Zstandard conversation payloads (decided).** Compress each
  conversation event's Pydantic JSON payload with Zstandard in
  `utils.compress` and decode it with `utils.decompress`. Remove
  Brotli from runtime dependencies; dispatch actions remain text. No
  migration or legacy payload decoding is provided before production.
- **D60 — Terminal result output (decided).** Remove the `to_stderr`
  option from `Chat`'s reader methods. Both startup catch-up results
  and results for a new one-shot turn print to stdout, simplifying the
  reader path. The CLI still prints a newly generated conversation ID
  to stderr in one-shot mode.
- **D61 — Chat reader cursor (decided).** Keep the terminal consumer
  group's read cursor on each `Chat` instance. Start at `0` to drain
  that consumer's pending entries, advancing after each handled entry;
  switch to `>` once the pending read is empty and use it for
  subsequent reads. A new `Chat` instance starts at `0` again to
  recover pending entries after a restart.
- **D62 — Chat reader setup and fast-forward mode (decided).** Create
  or reuse the terminal consumer group when `Chat` is
  initialized. `Chat.read(ffwd=True)` drains available pending and new
  events without blocking for startup catch-up; the default read waits
  up to one second for a new-event batch while preserving pending
  recovery. This replaces a caller-supplied block timeout with the two
  intended reader modes.
- **D63 — Stream group creation errors (decided).** Centralize
  consumer-group creation in `stream.py` for both dispatch and
  conversation streams. Treat Redis `BUSYGROUP` as an existing group
  and log it at debug level; log and propagate other Redis response
  errors. `Chat` and `Worker` call their stream's `create_group`
  without duplicating this policy.
- **D64 — Direct stream writes (decided).** Remove Redis pipelines,
  watched transactions, and the renewable agent-run lock from
  `stream.py` and `worker.py`. Write conversation events before the
  corresponding dispatch actions with direct `XADD` calls, and use
  ordinary event reads for sequential result fan-in. This favors a
  small implementation during development. Writes across streams are
  no longer atomic: a crash or Redis error between them may strand a
  turn or child, while concurrent workers may duplicate results,
  terminal events, resume actions, or model runs. Revisit these
  guarantees before depending on automatic recovery or concurrent
  worker correctness.
- **D65 — Stream append methods (decided).** Route dispatch writes
  through `DispatchStream.append_event(action_type, **fields)` and
  conversation writes through
  `ConversationStream.append_event(event_type, payload)`. Each method
  returns the decoded Redis entry ID. This keeps stream names and
  event encoding at their respective stream boundaries without
  changing write order or making related writes atomic.
- **D66 — Recoverable write order and duplicate-tolerant replay
  (proposed).** Persist enough conversation state before each
  dependent dispatch action so a future recovery command can recreate
  missing work. Recovery must reconcile incomplete turns and calls by
  their turn, step, and call IDs; the last entry of each stream is
  insufficient when turns and tool calls overlap. The current
  `turn.subagent.requested` event stores child IDs but not the agent,
  model, dependencies, or prompt, so it cannot rebuild a child after a
  partial creation. D74 defines the durable child intent needed before
  relying on recovery. Replay should choose one canonical terminal outcome and
  one result per call, ignoring later duplicates; `reconstruct_turn`
  does not yet do this for duplicate completed-turn history or
  repeated tool-request history.
- **D67 — Per-call request events (declined).** One conversation event
  per deferred tool call would pair each durable request with its
  dispatch action. The existing `turn.tool.requested` event already
  contains the full request set, so a recovery pass can derive every
  missing `turn.tool.call` action from it. Splitting the event would
  require a separate request-set manifest or equivalent expected-call
  list, and history and usage must be applied only once per model step.
  Otherwise a partially written set is indistinguishable from a
  complete one and current turn reconstruction would duplicate history
  and reset collected results. Keep one request-set event per model step.
- **D68 — Empty deferred request sets (decided).** Reject an empty
  `call_ids` list in `ConversationStream.queue_tool_requests` before
  writing an event. A deferred request checkpoint without calls has
  no tool work to dispatch; the former immediate resume action is
  removed.
- **D69 — Child conversation ID field (decided).** Name the child ID in
  `SubAgentRequestedPayload` `child_conversation_id` rather than the
  ambiguous `conversation_id`. `ParentToolReference.conversation_id`
  continues to identify the parent. This changes the serialized field
  directly; no legacy format is supported before production. The
  rename alone does not repair partial child creation; see D70.
- **D70 — Recovery checklist and child write order (decided).** Add a
  no-op `recover()` in `recovery.py` with the reconciliation work listed
  in comments. No recovery command is implemented yet. Write child
  `conversation.created`, child `turn.queued`, parent
  `turn.subagent.requested`, then child `conversation.poke`.
  After the child turn is queued, its stream has the configuration,
  prompt, turn ID, and parent reference needed to repair later writes.
  A crash after only `conversation.created` still needs safe rollback
  (for example XDEL of a proven orphan) and replay of the parent tool
  action; reordering alone cannot make every prefix forward-recoverable.
- **D71 — Cursor-based result fan-in (decided).** Keep the last conversation
  event ID in reconstructed turn state. When saving a tool result, combine
  the reconstructed results with only events written after that ID, then
  scan the short tail again after the append to include concurrent sibling
  results. Child-result actions carry the child's terminal event ID so their
  parent can read that event directly. This reduces repeated full-stream
  scans; the separate reads and writes still permit duplicate results or
  resume actions under concurrent workers.
- **D72 — Stable tool execution identity (decided; implementation pending).**
  Provide tools with enough context to derive a scoped idempotency key from
  the namespace, conversation ID, turn ID, step ID, and tool call ID.
  Replaying the same persisted tool request must preserve that identity;
  distinct requests must have distinct identities. Do not derive it from
  worker IDs, dispatch entry IDs, or execution attempts. Tool context already
  exposes conversation and tool call IDs; namespace, turn, and step context
  remain to be exposed. Tools can pass the key to downstream systems, such as
  an API's `Idempotency-Key`, so those systems can detect repeated execution.
  External-effect deduplication belongs to tools and downstream systems, not
  Fallingwater's worker recovery; this does not guarantee exactly-once
  effects or close R1.
- **D73 — Stateful turn machine (proposed).** Explore a turn machine that
  holds its current state and accepts events through a transition method,
  rejecting events that are invalid in that state. Replace the initial
  reconstruction/eligibility sketch in `contrib/turn-state-machine.py` with
  a minimal machine initially in `NotStarted`. `PromptSubmitted` transitions
  to `AwaitingAnswer(prompt)` and returns a poke message; `AgentAnswered`
  transitions to `Completed(prompt, answer)` and returns no messages.
  The caller publishes returned messages; the machine performs no Redis I/O.
  Durable replay, tool and child transitions, and
  concurrent checkpoint protection remain design work; the library is unchanged.
- **D74 — Durable parent-side child intent (decided; implementation pending).**
  Persist the full child delegation intent in the parent conversation stream
  before creating or dispatching the child. Include the child conversation
  and turn IDs, agent import path, effective model, serialized dependencies,
  prompt, and parent turn/step/tool-call correlation. Once this intent is
  saved, recovery must be able to create or complete missing child setup using
  the same identities without rerunning the parent tool to rediscover its
  delegation result. This makes the parent intent a durable checkpoint of that
  result and avoids orphan rollback for crashes during subsequent child setup.
  It does not cover a crash before the intent is saved. The event schema and
  idempotent child materialization remain to be implemented; D70 describes the
  current child-first write order, which this decision will replace.
- **D75 — Scoped message payload names (decided).** Rename
  `ToolRequestedPayload.history` to `step_messages` and
  `TurnCompletedPayload.history` to `turn_messages`. The former holds one
  model step's new messages; the latter holds all new messages accumulated
  during a completed turn, not a full conversation snapshot. Update event
  writers, reconstruction, and compact formatting together. Serialized event
  and archive field names change directly; no legacy aliases or migration are
  provided before production.
