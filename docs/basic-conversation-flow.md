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

Create Chat ---------------> conversation.created
  agent, model, deps

send(prompt)
  queue prompt + action
  (atomic transaction)
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

Creating a new `Chat` validates the selected agent and available model, then
appends `conversation.created`. This event stores the agent import path, model,
and initial dependencies. Creating the conversation does not dispatch an agent
run. Library-created conversations use a UUID by default; the CLI generates a
readable conversation ID.

`Chat.send(prompt)` generates a fresh UUID turn ID and writes two entries in
one Redis transaction:

1. Conversation: `turn.queued`, containing the turn ID and prompt message.
2. Dispatch: `conversation.poke`, containing the conversation and turn IDs.

The entries have their own Redis stream entry IDs but share the same turn ID.
`send` returns the turn ID and the queued event's entry ID as a cursor. A
successful return confirms queuing; it does not mean the agent has completed.
Every call submits a new turn, including repeated prompts.

A worker reads dispatch through the `workers` consumer group. It reconstructs
the selected turn from the conversation stream, loads the agent and
dependencies, and runs the agent with the prompt and saved message history.
The first turn has no earlier conversation history. The worker uses a
renewable per-step lease to suppress concurrent duplicate agent runs.

When the agent returns a final answer, the worker writes `turn.completed` with
the turn ID, display message, new structured message history, and usage. It
then acknowledges the dispatch action. For a basic conversation, completion
does not append a new dispatch action.

`Chat.receive(turn_id, cursor)` follows the conversation stream after the
cursor using `XREAD`. It returns the matching completed message or raises
`RuntimeError` for a matching failed turn. `Chat.ask(prompt)` combines submission
and result reading as `receive(*send(prompt))`. Reading the result does not
consume work from the dispatch stream.

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

Opening `Chat` with an existing conversation ID uses its saved agent and model,
ignoring supplied replacements. Initial dependencies remain in the creation
event; supplying new dependencies on resume is rejected. Resumption does not
append another `conversation.created` event.

The interactive CLI waits for each submitted turn's outcome before accepting
its next prompt. Independent clients can submit overlapping turns. A turn's
history includes completed events recorded before its queued event; an earlier
answer that arrives after submission is excluded. Clients that need sequential
history should wait for each turn to finish before submitting the next.

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
feeds use independent `XREAD` cursors instead.

Closing the client leaves submitted work in Redis. Workers recover abandoned
dispatch actions with `XAUTOCLAIM` after the configured idle period. A saved
terminal event lets a recovered action skip an already finished turn. A crash
before saving the outcome can cause the agent run to execute again.

Acknowledgement clears a dispatch action's pending status but does not delete
its stream entry. Conversation events also remain available for resumption,
history reconstruction, and monitoring. Dispatch entries carry an explicit
action `type` and text routing fields; conversation events carry a `type` and
a compressed JSON `payload`.
