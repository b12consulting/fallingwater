# Tool call flow

Deferred tool execution involves two Redis Streams. With the default `fw`
namespace:

- `fw:dispatch` is the shared stream of actions for workers.
- `fw:conversation:<conversation-id>:events` records the conversation,
  including tool requests and results.

Ordinary tool calls use the conversation's existing stream. A tool that returns
a `SubAgentRequest` creates child work as described in [subagent-flow.md](subagent-flow.md).

## Successful tool call

This diagram shows one tool call followed by an agent run that completes.
The agent may request additional tools before completing. The diagram starts
after client submission: `turn.queued` and `conversation.poke` already exist.
The worker lane represents whichever worker handles each action; successive
actions can run on different workers.
Arrows labelled "Acknowledge" refer to existing dispatch entries; they do not
append new actions.

```text
Conversation events           Shared dispatch              Workers
-------------------           ---------------              -------

turn.queued                   conversation.poke ----------> Claim action
                                                           Load turn; run agent
                                                           Receive tool requests
                                                           Save request + jobs
                                                           (successive writes)
turn.tool.requested <--------------------------------------+  |
                              turn.tool.call <---------------+
                              conversation.poke <--------- Acknowledge action

                              turn.tool.call -------------> Claim tool action
                                                           Execute tool
                                                           Receive result
                                                           Save result + resume
                                                           (successive writes)
turn.tool.result <-----------------------------------------+  |
                              turn.agent.resume <------------+
                              turn.tool.call <------------ Acknowledge action

                              turn.agent.resume ----------> Claim resume action
                                                           Run agent with result
turn.completed <------------------------------------------ Save final answer
                              turn.agent.resume <--------- Acknowledge action
```

The prompt is recorded as `turn.queued`, and a `conversation.poke` action is
appended to dispatch in a following write. A worker reconstructs the
turn from the conversation stream and calls the agent once.

Fallingwater's Pydantic AI hook defers attached function tools. Instead of
executing them during that agent run, the run returns `DeferredToolRequests`.
The worker writes one `turn.tool.requested` event containing the full request
set, the run's new message history, and usage. It assigns the request set a
`step_id`. Following writes append one `turn.tool.call` dispatch action
per requested call, carrying the conversation, turn, step, and tool-call IDs.

The worker acknowledges the agent action and becomes available for other work.
Another available worker, or the same worker on a later action, processes each
tool action independently. It loads the saved request, resolves the attached
function on the agent, and reconstructs a basic `RunContext` if required.
Synchronous and asynchronous function tools are supported. The worker remains
occupied until its current tool call returns.

An ordinary tool return value is converted to JSON-compatible data and saved
in a `turn.tool.result` event with the matching turn, step, and tool-call IDs.
For a single-call request set, that result completes the set, so a following
write dispatches `turn.agent.resume`.

The resume worker rebuilds the saved history and `DeferredToolResults`, restores
checkpointed usage, and calls the agent with no new user prompt. The agent
receives the tool result and either returns its answer or requests another
set of tools. A final answer is stored as `turn.completed`, including the
turn's accumulated new messages and usage.

## Parallel tool calls

One model step can request several tools. Fallingwater records one request-set
event and dispatches one action per call. With enough available workers, calls
can execute concurrently and finish in a different order from their requests.

```text
Conversation events           Shared dispatch              Workers
-------------------           ---------------              -------

turn.tool.requested           turn.tool.call [A] ----------> Worker 1 executes A
  requests: A, B, C           turn.tool.call [B] ----------> Worker 2 executes B
                              turn.tool.call [C] ----------> Worker 3 executes C

turn.tool.result [B] <------------------------------------- Worker 2 saves B
  waiting for A and C         turn.tool.call [B] <--------- Worker 2 acknowledges

turn.tool.result [C] <------------------------------------- Worker 3 saves C
  waiting for A               turn.tool.call [C] <--------- Worker 3 acknowledges

                                                           Worker 1 saves A
                                                           and then resumes
turn.tool.result [A] <-------------------------------------+  |
                              turn.agent.resume <------------+
                              turn.tool.call [A] <--------- Worker 1 acknowledges

                              turn.agent.resume ----------> Run agent with A, B, C
```

The request event and per-call dispatch entries are separate writes.
Result collection compares unique tool-call IDs against the saved request set.
After saving the final missing result, the worker queues one resume action.
Earlier results do not queue a resume. No worker stays blocked waiting for
sibling calls; after saving its own result, it acknowledges its action and
becomes available again.

## Failed tool call

```text
Conversation events           Shared dispatch              Workers
-------------------           ---------------              -------

                              turn.tool.call -------------> Claim tool action
                                                           Execute tool
                                                           Tool raises exception
turn.failed <--------------------------------------------- Save turn failure
                              turn.tool.call <------------ Acknowledge action
```

Under the current policy, a non-Redis exception during tool processing fails
the whole turn. The worker records `turn.failed` and acknowledges the tool
action. It does not save the exception as an ordinary `turn.tool.result` or
schedule an agent resume for that failure. Later sibling results are ignored
once the turn has a terminal event. Calls already executing may still finish
and produce external side effects.

Redis failures leave the action pending for recovery. If this conversation is
a child, saving its failure also dispatches `turn.subagent.result` to notify
its parent, following the subagent flow.

## Stream contents and recovery

Dispatch entries are worker actions with an explicit `type` and text routing
fields. Conversation entries are durable events with a `type` and a compressed
JSON `payload`. In particular, `turn.tool.call` and `turn.agent.resume` are
dispatch actions; `turn.tool.requested` and `turn.tool.result` are conversation
events. There is no separate tool-result dispatch action for ordinary tools.

Workers acknowledge each action after saving its checkpoint. Acknowledgement
clears pending status but does not delete the entry from `fw:dispatch`.
Abandoned actions can be recovered with `XAUTOCLAIM`; saved tool results and
terminal events allow recovered actions to skip work already checkpointed.
Agent steps have no execution lock; an active action reclaimed by another
worker can run twice.

Tool execution is not guaranteed to happen exactly once. A worker can crash
after an external side effect but before saving its result, causing recovery
to execute the tool again. Tool calls may also run twice if an active action
is reclaimed. Saved-result checks skip sequential duplicates, but concurrent
workers can still write duplicate results and resume actions.
