# RAID log

Track technical risks, assumptions, issues, and decisions that depend
on how Fallingwater is implemented. Record the status and reason for
each choice, including when to revisit it. Add a decision when a
technical choice is discussed or an implementation must be
revisited. Close or revise an item when the code resolves it.

## Risks

- **R1 — Duplicate external effects (open).** Workers skip results already
  visible when they load a turn. Cursor-checked Lua appends guard result
  fan-in, persistent registrations, and agent initialization.
  External execution remains unguarded, so concurrent workers can still
  duplicate model runs, tool effects, or dispatch actions. A crash
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
  tool-call lock, so both model calls and tools may run twice. D89 chooses
  automatic ownership renewal during processing; it is not implemented yet.
  Tool/network timeouts remain separately necessary for stuck calls.
  Increasing the interval above one HTTP timeout alone does not cover
  retries or long-running tools.
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
  action. Replaying a saved delegation can now complete child setup and
  republish a ready child's poke, but there is no general reconciliation
  scanner. Add repair or transactional writes before relying on unattended
  recovery.
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
- **I6 — Redis integration coverage (partially addressed).** Isolated
  real-Redis tests cover concurrent result saves and duplicate calls,
  concurrent named-agent registration through separate worker instances,
  and reader-group rewind with pending entries owned by different consumers.
  In-memory tests cover replay after interrupted child setup. End-to-end
  worker restart, dispatch reclamation, ownership loss, and full partial-write
  recovery still need real-Redis coverage.
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
- **D18 — CLI reader recovery (amended by D60, D98, and D99).** Use a
  consumer group on each conversation stream for one logical terminal reader,
  defaulting to `user` and configurable with `--group`. Reuse the
  stable `terminal` consumer name to read pending entries after
  restart. Print queued prompts and completed or failed outcomes before
  acknowledging them; acknowledge other events as read. On
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
- **D23 — Loaded conversation value (superseded by D83 and D87).**
  The former `turn_state.reconstruct_turn` returned a frozen Pydantic
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
  result. A tool can return `AgentRequest(agent_path, prompt,
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
- **D42 — Pure turn reconstruction (amended by D83 and D87).** Keep Redis
  pagination in `ConversationStream.iter_events` and fold ordered, decoded
  events in the pure `conversation.reconstruct_conversation`. This replaces
  the former single-turn reconstruction with all-turn replay, isolating
  workflow rules from Redis access; `Worker` supplies the stream iterator.
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
- **D48 — Typed dispatch actions (amended by D79).** `DispatchStream` decodes
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
  events directly. IDs stay outside serialized Redis payloads. D76 removes
  the tuple decoder for CLI and web inspection; D77 requires conversation
  event IDs in the unified model and includes them in archive records.
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
  and the payload registry in `models.py` (renamed by D78). Keep stream key
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
- **D64 — Direct stream writes (amended by D88 and D97).** Remove Redis
  pipelines, watched transactions, and the renewable agent-run lock from
  `stream.py` and `worker.py`. Write conversation events before their
  corresponding dispatch actions. The initial implementation used direct
  `XADD` calls and ordinary event reads for result fan-in to keep development
  simple, allowing duplicate results and resume actions under concurrency.
  D88 adds cursor-checked result appends; D97 extends
  them to delegation registration and child setup. General checkpoints
  retain prepare/append/publish without a cursor check, and publication
  across streams remains non-atomic. Concurrent workers can still duplicate
  terminal events, dispatch actions, or external execution, and a crash between
  writes can strand work. Revisit these guarantees before depending on automatic
  recovery or broader concurrent worker correctness.
- **D65 — Stream append methods (amended by D79).** Route dispatch writes
  through `DispatchStream.append_event(action_type, payload)` and
  conversation writes through
  `ConversationStream.append_event(event_type, payload)`. Each method
  returns the decoded Redis entry ID. This keeps stream names and
  event encoding at their respective stream boundaries without
  changing write order or making related writes atomic.
- **D66 — Recoverable write order and duplicate-tolerant replay
  (partially implemented by D83 and D97).** Persist enough conversation
  state before each dependent dispatch action so a future recovery command
  can recreate missing work. Recovery must reconcile incomplete turns and calls by
  their turn, step, and call IDs; the last entry of each stream is
  insufficient when turns and tool calls overlap. D97 implements D74's
  complete parent-side `turn.agent.requested` intent before child setup,
  allowing replay to finish missing creation or queueing without rerunning
  the parent tool. D83 implements canonical result/terminal replay and
  skips identical current request checkpoints, keeping one result per call
  and the first terminal outcome. General partial-write reconciliation
  remains unimplemented.
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
- **D68 — Empty deferred request sets (decided; validation moved by D83).** Reject an empty
  deferred call set in the `RequestTools` transition before
  writing an event. A deferred request checkpoint without calls has
  no tool work to dispatch; the former immediate resume action is
  removed.
- **D69 — Child conversation ID field (decided).** Name the child ID in
  `AgentRequestedPayload` `child_conversation_id` rather than the
  ambiguous `conversation_id`. `ParentToolReference.conversation_id`
  continues to identify the parent. This changes the serialized field
  directly; no legacy format is supported before production. The
  rename alone does not repair partial child creation; see D70.
- **D70 — Recovery checklist and child write order (partly superseded by D97).**
  Add a no-op `recover()` in `recovery.py` with the reconciliation work listed
  in comments. No recovery command is implemented yet. The original order was
  child `conversation.created`, child `turn.queued`, parent
  `turn.agent.requested`, then child `conversation.poke`.
  This child-first order could leave incomplete setup without a durable
  parent intent. D74/D97 replace it with parent intent first, followed by
  idempotent child creation, queueing, and dispatch. The no-op recovery
  placeholder remains; replay repairs setup only when the original tool
  action is processed again.
- **D71 — Cursor-based result fan-in (superseded by D88).** The original
  implementation kept the last replayed event ID, read newer results before
  saving a result, and scanned the tail again afterward for concurrent siblings.
  This read/append/read approach permitted duplicate results or resume actions;
  D88 replaces it with cursor-checked appends and replay through the exact
  successful append. Child-result actions still carry the child's terminal
  event ID so their parent can read that event directly, avoiding full-stream
  scans. Result persistence and dispatch publication remain separate writes.
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
- **D73 — Stateful turn machine (implemented by D83).** Explore a turn machine that
  holds its current state and accepts conversation payloads through a transition
  method, rejecting payloads that are invalid in that state. Replace the initial
  reconstruction/eligibility sketch in `contrib/turn-state-machine.py` with
  a minimal prompt-to-answer machine. D82 replaces its explicit state classes
  with transition models holding payload endpoints. Accepting
  `TurnQueuedPayload` returns a `ConversationPokePayload`; accepting
  `TurnCompletedPayload` finishes the turn and returns `None`.
  The machine rejects payloads for another turn and invalid state/payload pairs.
  Callers pass a decoded event's payload; Redis IDs and type envelopes remain
  outside the machine.
  The caller publishes returned payloads; the machine performs no Redis I/O.
  D83 integrates durable replay and parallel tool/child transitions.
  General checkpoint protection remains design work.
- **D74 — Durable parent-side child intent (implemented by D97).**
  Persist the full child delegation intent in the parent conversation stream
  before creating or dispatching the child. Include the child conversation
  and turn IDs, agent import path, effective model, serialized dependencies,
  prompt, and parent turn/step/tool-call correlation. Once this intent is
  saved, recovery must be able to create or complete missing child setup using
  the same identities without rerunning the parent tool to rediscover its
  delegation result. This makes the parent intent a durable checkpoint of that
  result and avoids orphan rollback for crashes during subsequent child setup.
  It does not cover a crash before the intent is saved. `AgentRequestedPayload`
  now holds that specification; cursor-checked registration and child
  materialization replace D70's child-first order. Reprocessing a saved
  delegation invokes `ensure_subagent` without executing the parent tool again,
  validates existing settings and queued requests, and republishes a poke
  when the child turn is ready. This is replay repair, not a general recovery
  scanner or exactly-once dispatch guarantee.
- **D75 — Scoped message payload names (decided).** Rename
  `ToolRequestedPayload.history` to `step_messages` and
  `TurnCompletedPayload.history` to `turn_messages`. The former holds one
  model step's new messages; the latter holds all new messages accumulated
  during a completed turn, not a full conversation snapshot. Update event
  writers, reconstruction, and compact formatting together. Serialized event
  and archive field names change directly; no legacy aliases or migration are
  provided before production.
- **D76 — One decoder for conversation stream entries (amended by D77).** Remove
  `decode_conversation_entry` and its tuple return in favor of
  `decode_conversation_event(entry_id, fields)`. CLI inspection, web
  feeds, and turn reconstruction use `ConversationStreamEvent`, with cursor
  and SSE IDs taken from its `entry_id`. Keep the ID-free `ConversationEvent`
  for payload decoding and archives; Redis entry IDs remain excluded from
  serialization. This changes the internal read representation, not stored
  events, archive records, or CLI/SSE output.
- **D77 — Unified conversation event model (decided).** Fold payload decoding
  into one `decode_conversation_event(entry_id, fields)` function, named
  consistently with `encode_conversation_event`. Remove the former ID-free
  decoder variant and `ConversationStreamEvent`. Use
  `ConversationEvent` everywhere, with a required nonempty `entry_id`.
  Archive records now store and require the source ID; no legacy ID-free
  archive support is provided. Restore validates the archived IDs but continues
  to append events with new Redis-generated IDs. Redis payload encoding and
  CLI/SSE output remain unchanged. D79 replaces the dispatch action model
  with a typed payload and event envelope.
- **D78 — Models module name (decided).** Rename `stream_models.py` to
  `models.py` and update library and test imports. Keep event and dispatch
  schemas and their payload registry together; codecs remain in
  `stream_utils.py`. The module rename does not change serialized data.
- **D79 — Dispatch payloads and event envelopes (decided).** Separate
  `ConversationPokePayload`, `AgentResumePayload`, `ToolCallPayload`, and
  `SubAgentResultPayload` from Redis metadata. Their union is
  `DispatchPayloadType`; `DispatchEvent` contains the required `entry_id`,
  action `type`, and typed `payload`, validating the type/payload pairing.
  State-machine transitions can return one payload or `None` without inventing
  a Redis ID. `DispatchStream.append_event` takes the action type and payload;
  dispatch codecs preserve the existing flat Redis wire format. Workers route
  payloads and acknowledge event IDs. Malformed entries use
  `InvalidDispatchEvent`. Event-envelope serialization includes the ID, while
  dispatch payloads and Redis fields do not.
- **D80 — Event-model payload validation (decided).** Validate type/payload
  pairing on both `ConversationEvent` and `DispatchEvent`, including direct
  construction and archive deserialization. Remove duplicate pairing checks
  from encoders and explicit payload-model selection from decoders. Encoders
  serialize the supplied type and payload; decoders rebuild the envelope and
  delegate validation to its model. Keep wire-envelope and compression/JSON
  checks in codecs, and preserve malformed dispatch entries for acknowledgement.
- **D81 — Tool and child turn-machine sketch (proposed).** Add
  `contrib/turn-state-machine2.py` with parent and child machines driven by
  conversation payloads. Keep one dispatch payload or `None` per transition
  by modeling sequential tool calls, not production parallel fan-out. Child
  terminal outcomes emit a parent poke; the caller reads the saved child
  outcome and supplies its payload to the waiting parent. This avoids requiring
  a terminal Redis event ID inside a payload-only transition. The sketch
  retains the current child-failure propagation policy and leaves history,
  durable child intent (D74), replay, and concurrency outside its scope.
  Like the base example, Pydantic transition models now hold start/stop
  payloads, validate endpoint correlation, and implement `emit`; a TypeAdapter
  selects the edge. Request and per-call results remain machine context across
  edges. Child-outcome edges compare the delegated child turn ID and normalize
  completion to a parent tool result or failure to a parent failure, so a child
  terminal payload does not mistakenly finish its parent.
- **D82 — Payload-instance transition models (proposed).** In
  `contrib/turn-state-machine.py`, define Pydantic transition models with
  narrowly typed `start` and `stop` fields holding actual payload instances.
  The model classes define allowed edges; each instance records a particular
  transition. A `TypeAdapter` over the model union selects and instantiates the
  valid transition from the current and incoming payloads. Declare `None` to
  queued, queued to completed, and completed to `None`.
  The base transition validator compares turn IDs only when both endpoints are
  present. The first transition has no starting turn ID to compare.
  The machine stores the latest payload and validates incoming payloads against
  these edges. Completion is recognized from the retained completed payload;
  `FinishTurn` models its terminal boundary without an additional input.
  Each transition declares
  an `emit` method returning a dispatch payload or `None`; the machine calls
  the matched edge's method, which can access its payloads, before updating the
  current payload. The terminal
  boundary remains a marker rather than an extra executed transition.
  The tool/child sketch follows the same transition-model architecture, with
  machine context for data spanning multiple edges.
- **D83 — Production payload-transition machines (decided and implemented).**
  Integrate the transition-model architecture into `turn_state.py`, preserving
  production parallel fan-out and exact-ID child-result dispatch rather than
  the sequential scheduling and parent pokes of D81. `Conversation` stores its
  settings, ID, replay cursor, and `turn_state: dict[str, TurnStateMachine]`;
  per-turn prompt/history/usage, requests, results, and delegations live on the
  machines. Replay builds every turn and snapshots earlier completed history
  and usage at each queued event, including overlapping turns. History snapshots
  share immutable prefixes so replay does not retain quadratic full-history
  copies. Conversation-private replay accumulators index the latest prefix
  and completed turn without duplicating message contents or usage values.
  Transition `emit()` methods return tuples for zero, one, or many actions.
  A parent notification intent is bound to the generated Redis terminal ID
  after the terminal write. Stream helpers handle persistence and concurrent
  tail refreshes; workers ask machines for ready/waiting/stale eligibility,
  execute enabled work, and checkpoint its payload.
  Checkpoint helpers preview transitions without changing their reconstructed
  snapshot; only stream replay advances it. This prevents a just-written outcome
  from hiding an earlier concurrently persisted result or terminal event.
  Keep strict validation for new transitions but use the first persisted result
  per call and the first terminal outcome during replay. Ignore identical
  current request checkpoints without repeating messages or resetting results,
  obsolete results, and late work after a terminal outcome. Child results are
  normalized through child transitions without importing the parent's agent.
  This integration introduced no Redis transaction, lock, recovery implementation,
  or wire-schema change. D88 later adds cursor-checked result saving and D97
  implements D74's durable delegation setup; D72 remains pending. A future
  recovery pass should enumerate enabled actions from machines and reconcile
  missing writes. General checkpoints still lack cursor checks, and cross-stream
  checkpoint/dispatch bundles are not atomic.
- **D84 — Transition context validation (decided and implemented).** Add
  `Transition.validate_context(machine)` and move agent-readiness, pending-call,
  repeated-step, and delegation guards out of `emit()` into that hook. Shared
  readiness and pending-call helpers belong to the base transition rather than
  `TurnStateMachine`. `preview()` validates context before constructing emissions;
  child-outcome normalization validates context without emitting unused actions.
  Keep dispatch eligibility, replay filtering, and stream-envelope checks
  unchanged. The machine remains a plain Python class; this does not introduce
  Pydantic machine-wide or assignment validation.
- **D85 — Derived turn status properties (decided and implemented).** Replace
  `TurnStateMachine.done` with `completed`, `failed`, and `running`, derived from
  the current payload rather than stored separately. Running is the complement
  of completed or failed, including an unstarted machine and turns waiting for
  tools or children; it does not indicate active worker execution. Preserve
  terminal replay/dispatch behavior and add history only when a running turn
  becomes successfully completed. This does not implement retries or reopen
  failed turns.
- **D86 — Pure conversation decisions and low-level streams (implemented).**
  Move workflow decisions behind `Conversation`, shared by `Chat` and `Worker`,
  while retaining turn-specific validation on its machines. `prepare` returns
  a validated payload and dispatch intents without advancing canonical state.
  New-turn preparation does not register a machine; only replay captures its
  history and usage. Parent notification intents bind the actual persisted
  terminal ID outside Redis access. Streams accept supplied payloads and handle
  wire types, codecs, reads, writes, groups, claims, and acknowledgements; they
  no longer construct workflow payloads or call domain machines.
  Worker owns checkpoint and child-write sequencing, including result replay
  before validation and after persistence before deciding fan-in. Chat uses
  the same preparation abstraction for submission without loading full history.
  Import-boundary and no-I/O preparation tests guard the separation; existing
  race and write-order tests retain consistency coverage. This supersedes
  D83's stream-owned orchestration without changing wire schemas or public
  Chat behavior. Former high-level `ConversationStream` workflow helpers are
  removed; low-level `append_event` remains available. No atomicity,
  exactly-once execution, or recovery guarantees are added.
- **D87 — Separate conversation and turn-state modules (implemented).**
  Extract `Conversation`, `PreparedEvent`, and `reconstruct_conversation` to
  `conversation.py`. Keep payload transitions, notification intents, shared
  message prefixes, and per-turn machines in `turn_state.py`. Conversation
  depends on turn state, never the reverse; both remain I/O-free and streams
  depend on neither. Update callers and import-boundary checks without
  compatibility re-exports or behavior changes.
- **D88 — Cursor-checked tool-result append (implemented).**
  `ConversationStream.append_if_current` uses a single-key Lua script to check
  for events after the caller's replay cursor. If stale, return up to 100
  events without writing; otherwise append the supplied encoded payload and
  return that exact event. Worker replays returned events and revalidates
  eligibility before retrying. On success, decide resumption from turn state
  replayed through this append's ID, without refreshing later events. This
  replaces D86's post-write tail refresh: two concurrent result savers cannot
  both observe the completed tool set at their own append boundary, and a
  duplicate saver rejects its result after replaying the winning write.
  Lua remains domain-agnostic and event wire formats do not change. Isolated
  real-Redis tests cover Lua responses, bounded conflicts, errors, simultaneous
  distinct results, and duplicate calls. Result/dispatch writes remain separate,
  external effects are not exactly-once, and retries can contend under sustained
  conversation traffic; R1 and R8 remain open.
- **D89 — Automatic renewal of active dispatch ownership (decided, not
  implemented).** Renew a pending entry's idle time periodically while its
  worker processes the action, from delivery or claim through checkpointing,
  dispatch publication, and acknowledgement. Renewal runs independently of
  blocking model/tool execution so healthy long-running work is not reclaimed
  solely because it exceeds `claim_idle_ms`. Atomically verify the entry still
  belongs to this consumer before renewing it; renewal must not claim it back
  after another worker takes ownership or recreate an acknowledged entry.
  Stop renewal when processing ends so genuinely abandoned work remains
  recoverable.
  Before implementation, define renewal timing, compatible claim thresholds,
  failure/lost-ownership handling, and ownership-checked writes and
  acknowledgements that prevent stale workers from completing reclaimed work.
  Test long-running work, stopped renewal, and ownership-loss races against
  real Redis. Renewal is not a tool execution deadline: a hung network call can
  remain owned indefinitely while renewal is healthy. Explicit network
  timeouts, bounded retries, and any hard tool deadline are separate decisions;
  timing out a Python thread does not stop its execution. Remote side effects
  already started may continue, so renewal does not promise exactly-once
  external effects or close R1, R5, or R8 before implementation and validation.
- **D90 — Server-rendered recent conversation dashboard (implemented).**
  Add `/dashboard/` to the web router, mounted at `/fw-api/dashboard/` by the
  standalone app. Do not add root-level dashboard routes or redirects.
  Reuse monitor discovery and its ten-event tails, rendering conversation cards
  and color-coded slots with htmx 4 and Bootstrap 5. Poll an HTML card fragment
  every two seconds with focus-preserving morphing instead of adding a second
  SSE protocol or client-side conversation store. Fetch complete event details
  by exact stream ID into a modal outside the polling region. Escape payload
  content, validate event IDs, and surface request and asset-loading failures.
  Pin integrity-checked CDN assets without introducing a frontend build.
  This bounds each page refresh to ten conversations and ten events each;
  CDN access and periodic Redis reads are the tradeoffs. Existing monitor and
  SSE routes are unchanged.
- **D91 — Full retained conversation history with inline details (implemented).**
  Link overview conversation names to `/fw-api/dashboard/{conversation_id}` and
  reuse the same card and event detail templates. Read every retained stream event,
  laying out rows of at most ten slots within the overview's card width. Poll
  only the history card, keeping the selected details in a separate column
  rather than a modal. This also supports conversations outside the recent set.
  Move card and event fragments to `/dashboard-cards` and `/dashboard-event`,
  avoiding collisions between fragment paths and conversation IDs.
  Full-history reads and HTML responses grow with stream length; pagination
  would change the requested all-events view and is not introduced here.
- **D92 — Historical cutoff for the dashboard overview (superseded by D94).**
  Interpret the date/time picker as an inclusive upper bound, not a filter for
  activity since the selected date. Apply the normalized UTC millisecond ID
  to dispatch discovery, event tails, and initial settings reads, including all
  sequence IDs in the cutoff millisecond. Preserve the ten-conversation and
  ten-event limits, dispatch pagination, and cutoff during polling. An empty
  selection restores live reads. Keep monitor/SSE and full-history detail pages
  unchanged. This browses retained stream data only; trimmed events or dispatch
  history cannot be recovered by the picker.
- **D93 — Button-triggered dashboard date selection (superseded by D94).**
  Keep Bootstrap 5 and the native date/time input, placing the input inside a
  dropdown opened by a calendar button. Format the button's selected date as
  `yyyy/mm/dd`, since the native input's presentation follows browser/OS locale.
  This preserves the UTC cutoff and live reset without an additional calendar
  dependency; Bootstrap 6's datepicker example requires a different stack.
- **D94 — Calendar selection and exclusive Redis-ID history navigation
  (implemented).** Use Bootstrap 6 alpha and its bundled Vanilla Calendar Pro
  dependency for the requested actual button-triggered calendar, migrating
  event modals to native Bootstrap dialogs. Replace the time input and numbered
  day pagination with an exclusive `before=<redis-id>` cutoff. Date selection
  means midnight UTC; **< Prev** uses the oldest selected dispatch-poke ID.
  Discovery scans backward in batches of 100 for ten distinct conversations,
  preserving exact sequence ordering within a millisecond without counting
  the whole day's history. A conversation may recur through older pokes.
  Bound event and initial settings reads by milliseconds, not dispatch
  sequences, which are stream-local; exclude the entire boundary millisecond
  for `-0`, otherwise include its event sequences. Retain the cutoff while
  polling, synchronize the next Prev cursor from fragments, and update URLs
  for date selection, Prev, and Live. Replace the current URL rather than
  adding a competing browser-history handler alongside htmx. This supersedes D92's
  inclusive ISO-time parameter and D93's native-input dropdown. Bootstrap 6
  alpha stability, CDN access, retained dispatch history, and non-atomic
  cross-stream timelines remain explicit tradeoffs.
- **D95 — Focused web read and presentation refactoring (implemented).**
  Normalize dashboard cutoffs once into an immutable value with distinct
  dispatch and event bounds, retaining D94's stream-local sequence rule.
  Name discovery's ordered conversation-to-poke map `latest_pokes` so card
  ordering and Prev are explicit. Move card/event formatting and typed template
  data to the I/O-free `web/dashboard.py` module; keep query validation,
  HTTP errors, and read coordination in the router.
  Extend `ConversationStream` with bounded forward reads, optional unlimited
  counts, and typed newest-first reads. Use those operations for dashboard
  tails, settings, exact events, full history, and the single-conversation SSE
  feed without changing existing stream-call defaults. Keep shared dispatch
  discovery and the recent feed's multiplexed read/eviction coordination
  unchanged. This does not change selection order, add an activity index, alter
  wire formats, or introduce cross-stream snapshot guarantees.
- **D96 — Durable failure tracebacks (implemented).** Store formatted Python
  tracebacks, including chained exceptions, in `TurnFailedPayload.traceback`
  alongside the existing error description so diagnostics survive worker-log
  loss. Preserve the child's traceback when its failure propagates to a parent.
  Default to null for older events and failures without a captured exception.
  Tracebacks can expose source paths and exception details; they are retained
  and exposed wherever complete conversation payloads are available.
- **D97 — Explicit persistent named agent sessions (implemented).**
  Add optional `name` and opt-in `persistent=True` to `AgentRequest`.
  Ephemeral requests retain fresh-conversation behavior even when named.
  Persistent creation uses the explicit name or agent definition's name;
  name-only follow-ups reuse the registered session and reject unknown names
  or conflicting explicit settings. Replay parent delegation events into
  `Conversation.agent_by_name` and resolved specifications, rather than storing
  mutable state in Deps or making the model track raw conversation IDs.
  Keep answer-only results and store parent routing on each queued agent turn.
  Persistent agent turns may overlap and retain queue-time history/usage
  snapshots. The initial automatic FIFO policy was removed to keep agents
  aligned with ordinary conversations; callers order dependent follow-ups by
  waiting for results. Each new turn dispatches independently.
  Reuse cursor-checked appends for atomic registration and idempotent setup.
  General checkpointing retains the simpler prepare/append/publish sequence;
  cursor-checked retry and refresh are deferred. Concurrent checkpoints can
  append competing outcomes, with the first terminal outcome retained on replay.
  Replaying a saved delegation can finish
  missing initialization without rerunning its tool, but a full reconciliation
  scanner and ownership renewal remain unimplemented. This replaces the
  initial raw-ID/reject-overlap proposal. The runnable
  `fallingwater/demo/editor_writers.py` demonstrates creation and named reuse.
  Persistence means reusable identity, not stream retention or exactly-once
  external effects; closure/cancellation APIs remain out of scope.
  The delegation payload is `AgentRequestedPayload`, registered under
  `Event.TURN_AGENT_REQUESTED` with wire name `turn.agent.requested`. The old
  request model and payload/event names have no compatibility aliases or
  migration.
  The former `sequential` creation setting is removed without migration.
- **D98 — Nonblocking terminal input and timestamped replies (implemented).**
  Use synchronous `prompt_toolkit` input in the main thread and preserved edits
  while one background thread reads the existing conversation consumer group.
  Replace the initial asyncio orchestration with a stop event and joined reader
  thread. Queue reader errors and invalidate the prompt; its render hook ends
  input so the main thread can raise the saved error.
  Submission does not wait for an answer; reader and submission failures
  propagate, and EOF/Ctrl-C stops and joins the reader without cancelling
  submitted work. Keep `Chat.ask`, `receive`, and one-shot CLI behavior blocking.
  Derive submission labels from the queued Redis ID in local time. At formatting
  time, use `HH:MM:SS` for today's date and `YYYY/MM/DD HH:MM:SS` otherwise.
  Cache that label and reuse it for replies and failures, rather than displaying
  completion time. Put each unstyled timestamp on its own line, right-aligned
  to the current terminal width, without brackets or a `reply to` prefix.
  Prefix queued message bodies with `SESSION_PROMPT` and highlight only those
  bodies with a light-grey background and contrasting dark text; replies and
  failures have no additional speaker label or background.
  Let the terminal wrap message bodies naturally,
  without padding rows or inserting width-dependent line breaks; measure width
  only for timestamp alignment. Use `prompt_toolkit` formatted output so styling
  does not leak into replies.
  Use the same formatted output for interactive and one-shot readers without
  an instance-wide display mode. Remember queued-event timestamps and scan
  retained events only when a missing timestamp must be recovered, including
  acknowledged prompts whose answers remain unread. These scans grow with
  conversation history.
  Timestamps can coincide and change with the local time zone; they are display
  labels only, while routing retains turn IDs. No automatic serialization or
  history refresh is introduced for overlapping turns.
- **D99 — Explicit reader-group rewind (implemented).**
  Add `fw chat --rewind` to reset the selected conversation group with
  `XGROUP SETID` at `0`, then read at `>` rather than replaying pending entries
  first. This displays retained conversation events once in stream order,
  even when later outcomes remain pending under the same or another consumer.
  Keep the existing group and pending entries; acknowledge replayed entries
  normally. Apply rewind before interactive or one-shot startup, without
  changing conversation data, other groups, or worker dispatch. The position
  is shared by all readers in the selected group; independent replays should
  use another group. This replays saved output, not agent or tool execution.
- **D100 — Separate conversation client and terminal UI (implemented).**
  Extract `ConversationClient` into `conversation_client.py` for opening and
  validating settings, turn submission, blocking results, reader-group cursors,
  acknowledgements, rewind, and retained-prompt lookup. `Chat(client)` owns
  terminal input, rendering, formatted timestamp caches, and the managed reader
  thread. The CLI constructs both explicitly; library callers use the client's
  `send`, `receive`, and `ask` rather than Chat methods.
  Keep `Conversation` pure: prepare each fresh turn from settings without
  loading history or retaining a misleading unreplayed conversation snapshot.
  Group reads yield typed events without automatic acknowledgement; Chat
  acknowledges through the client after rendering succeeds. Cache raw queued
  event IDs in the client and formatted labels in Chat. Preserve pending-first
  reads, rewind ordering, overlapping input, and one-shot output. This updates
  D86 and D98's client ownership without restoring the former combined
  conversation/worker-dispatch client described in D24/D25. It adds no recovery,
  atomicity, or shared mutable replay state between threads.
