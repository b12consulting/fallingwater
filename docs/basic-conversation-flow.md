# Basic conversation flow

A basic conversation involves a client, a worker, and two Redis Streams.
With the default `fw` namespace:

- `fw:dispatch` is the shared stream of actions for workers.
- `fw:conversation:<conversation-id>:events` records the conversation's
  configuration, prompts, outcomes, and message history.

The client submits work and reads results through Redis. The worker runs the
Pydantic AI agent separately, so it can finish a turn after the client
disconnects. This document describes a turn that returns an answer directly.
Tool and subagent execution extend this flow; see [tool-call-flow.md](tool-call-flow.md)
and [subagent-flow.md](subagent-flow.md).

## Creation and first turn

The worker lane represents whichever worker handles the dispatched turn.
Arrows labelled "Acknowledge" refer to existing dispatch entries; they do not
append new actions.

```text
Client                       Conversation events       Shared dispatch          Workers
------                       -------------------       ---------------          -------

Create client -------------> conversation.created
  agent, model, deps

send(prompt)
  queue prompt + action
  (two writes)
       +-------------------> turn.queued
       +---------------------------------------------> conversation.poke
  return turn_id, cursor

                                                       conversation.poke ------> Claim action
                             Saved configuration,
                             prompt, earlier history --------------------------> Load turn
                                                                                 Run agent

receive(turn_id, cursor) ---> XREAD after cursor
  wait for outcome                                                               Save answer,
                             turn.completed <----------------------------------- history, and usage
       <-------------------- Read matching outcome
  return answer                                        conversation.poke <------ Acknowledge action
```

Creating a new `ConversationClient` validates the selected agent and available
model, then appends `conversation.created`. This event stores the agent import path, model,
and initial dependencies. Creating the conversation does not dispatch an agent
run. Library-created conversations use a UUID by default; the CLI generates a
readable conversation ID.

`ConversationClient.send(prompt)` generates a fresh UUID turn ID and writes two
entries in order:

1. Conversation: `turn.queued`, containing the turn ID and prompt message.
2. Dispatch: `conversation.poke`, containing the conversation and turn IDs.

The entries have their own Redis stream entry IDs but share the same turn ID.
They are separate writes, so a failure between them can leave a queued turn
without a dispatch action.
`send` returns the turn ID and the queued event's entry ID as a cursor. A
successful return confirms queuing; it does not mean the agent has completed.
Every call submits a new turn, including repeated prompts.

A worker reads dispatch through the `workers` consumer group. It reconstructs
the conversation's turn machines from the stream, selects the dispatched turn,
asks its machine whether the action is enabled, then loads the agent and
dependencies, and runs the agent with the prompt and saved message history.
The first turn has no earlier conversation history. Concurrent workers may
run the same turn if a dispatch action is reclaimed while its first worker
is still active.

When the agent returns a final answer, the worker writes `turn.completed` with
the turn ID, display message, new structured message history, and usage. It
then acknowledges the dispatch action. For a basic conversation, completion
does not append a new dispatch action.

`ConversationClient.receive(turn_id, cursor)` follows the conversation stream
after the cursor using `XREAD`. It returns the matching completed message or
raises `RuntimeError` for a matching failed turn.
`ConversationClient.ask(prompt)` combines submission and result reading as
`receive(*send(prompt))`. Reading the result does not consume work from the
dispatch stream.

`Chat(client)` adds terminal input and rendering around a `ConversationClient`.
The client owns Redis access and reader-group operations; neither its submission
nor its result-reading methods print output. `Conversation` remains the pure
workflow model used to prepare turns and reconstruct persisted state.
`Conversation(events, conversation_id=...)` replays the supplied iterable by
passing it to `apply(events)`. `apply_one(event)` wraps a single event and calls
`apply`. Both replay settings and turn events without emitting dispatch actions.
An empty iterable is valid and leaves settings unset; preparing or applying a
turn before settings raises an error. Batches are consumed lazily and in order;
if an event or its source fails, the successfully applied prefix remains and
the error propagates.

## Client state snapshot

Opening a client replays all retained events into `client.conversation`,
including events already acknowledged by its reader group. To update that
snapshot later, explicitly call `refresh()`:

```python
client = ConversationClient(redis, conversation_id="my-conversation")
answer = client.ask("Hello")
client.refresh()
turns = client.conversation.turn_state
```

Submission, `read`, `receive`, and `ask` do not update this snapshot themselves.
`refresh()` reads events after `client.conversation.last_event_id` and applies
them in stream order without dispatching work or acknowledging deliveries.
This replay cursor is independent of the consumer group's delivery position.
Rewind replays output only; it does not reset or reapply the conversation state.
The snapshot is accurate through its last replayed ID, not necessarily through
the current stream head.

Concurrent refreshes and submission preparation are serialized internally.
Treat the exposed conversation as read-only and coordinate inspection if
another thread explicitly refreshes it. Opening replays the entire retained
history, increasing startup time and memory use; replay and Redis errors
propagate rather than returning an incomplete successful snapshot.

## Later turns and resumption

```text
Conversation events                      Workers
-------------------                      -------

conversation.created                     agent, model, deps
turn.queued    [turn 1]
turn.completed [turn 1] ----------------> turn 1 message delta
turn.queued    [turn 2] ----------------> next prompt
                                                |
                                         Run turn 2 with
                                         turn 1 history
                                                |
turn.completed [turn 2] <---------------- new message delta
turn.queued    [turn 3] ----------------> next prompt
                                                |
                                         Run turn 3 with
                                         turn 1 + turn 2 history
```

Each completed event stores the new messages for its turn, rather than a full
conversation snapshot. The worker combines completed message deltas in stream
order to reconstruct history for later turns. It also restores the latest
earlier completed usage, so usage accumulates across sequential turns.
Replay retains the first terminal outcome for each turn, so duplicate
completions do not repeat its history.

Opening `ConversationClient` with an existing conversation ID uses its saved
latest agent and model, ignoring supplied replacements. Dependencies come from
the latest saved settings; supplying new dependencies on resume is rejected.
Resumption does not append another `conversation.created` event.

The first event must be a `ConversationCreatedPayload`, still stored under
`conversation.created`. Additional payloads of that type are allowed and replace
the conversation's settings without resetting turns or history. Workers use the
latest replayed agent, model, and dependencies whenever they execute an agent
step or tool call, even for an already queued turn. This does not change an
invocation already in progress, queue-time message history, or a queued turn's
parent-routing reference. Changing agents can make outstanding tool calls
incompatible; those execution errors propagate normally.

The interactive CLI accepts new prompts without waiting for earlier outcomes.
A background reader prints answers or failures while `prompt_toolkit` preserves
the current input. Each prompt shows its queued event's local timestamp, and
its outcome references that same timestamp. Timestamp headers are right-aligned
on their own lines above the messages. A turn's history includes completed
events recorded before its queued event; an earlier answer that arrives after
submission is excluded. Clients that need sequential history should wait for
each turn to finish before submitting the next.

## Failed turn

```text
Client                       Conversation events       Shared dispatch          Workers
------                       -------------------       ---------------          -------

                                                       conversation.poke ------> Claim action
                                                                                 Load turn; run agent
                                                                                 Processing raises
                                                                                 a non-Redis exception
                             turn.failed <-------------------------------------- Save turn failure
receive raises <------------ Read matching outcome
  RuntimeError                                         conversation.poke <------ Acknowledge action
```

After reconstructing trusted turn state, a non-Redis processing exception is
saved as `turn.failed`, containing the turn ID and error text. The worker then
acknowledges the action. A failed turn does not supply a completed message
delta for later history.

Redis failures leave the action pending for recovery. Malformed dispatch
actions, or actions whose turn cannot be reconstructed, are logged and
acknowledged without creating a failure event because trusted turn state is
unavailable.

## Result readers and recovery

The CLI uses a conversation consumer group, defaulting to `user`, with the
stable consumer name `terminal`. On startup it reads pending and then new
events, printing unread answers or failures before accepting another prompt.
It prints results before acknowledging them, so an interrupted session can
print a result again after restart. Startup catch-up does not wait for turns
whose results have not yet arrived. The library's `receive` and the web event
feeds use independent `XREAD` cursors instead. `ConversationClient.read` yields
typed events for Chat to render and explicitly acknowledge after successful
handling. The client caches raw queued event IDs and scans saved events on a
cache miss, including already acknowledged prompts; Chat owns formatted
timestamp labels. `ConversationClient.ask`
and the CLI's `--msg` mode remain blocking. One-shot output uses the same
timestamp headers, message labels, and queued-message styling as interactive
chat, while `ConversationClient.ask` returns the answer without printing.

`fw chat <id> --rewind` resets the selected reader group's delivery position
and replays retained events in stream order, including already acknowledged
messages. It then continues normal chat, without rerunning agents or tools.
Readers sharing that group also share the reset; other groups are untouched.

Closing the client leaves submitted work in Redis. Workers recover abandoned
dispatch actions with `XAUTOCLAIM` after the configured idle period. A saved
terminal event lets a recovered action skip an already finished turn. A crash
before saving the outcome can cause the agent run to execute again.

Acknowledgement clears a dispatch action's pending status but does not delete
its stream entry. Conversation events also remain available for resumption,
history reconstruction, and monitoring. Dispatch entries carry an explicit
action `type` and text routing fields; conversation events carry a `type` and
a compressed JSON `payload`.
