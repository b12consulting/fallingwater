# Subagent flow

Subagent work involves three Redis Streams. With the default `fw` namespace:

- `fw:dispatch` is the shared stream of actions for workers.
- `fw:conversation:<parent-id>:events` records the parent conversation.
- `fw:conversation:<child-id>:events` records the child conversation.

## Successful child turn

This diagram shows one child that returns an answer, followed by a parent run
that completes. Either agent may request additional tools before completing.
The diagram starts after client submission: the parent's `turn.queued` and
`conversation.poke` already exist. The worker lane represents whichever worker
handles each action; successive actions can run on different workers.
Arrows labelled "Acknowledge" refer to existing dispatch entries; they do not
append new actions.

```text
Parent conversation        Child conversation         Shared dispatch              Workers
-------------------        ------------------         ---------------              -------

turn.queued                                           conversation.poke [parent] -> Run parent agent
                                                                                   Save request + job
                                                                                   (successive writes)
turn.tool.requested <--------------------------------------------------------------+  |
                                                      turn.tool.call <---------------+
                                                      conversation.poke [parent] <- Acknowledge action

                                                      turn.tool.call -------------> Execute parent tool
                                                                                   Get AgentRequest
                                                                                   Create child work
                                                                                   (successive writes)
turn.agent.requested <-------------------------------------------------------------+  |
                           conversation.created <---------------------------------------+
                           turn.queued <------------------------------------------------+
                                                      conversation.poke [child] <----+
                                                      turn.tool.call <------------- Acknowledge action

                                                      conversation.poke [child] ---> Run child agent
                                                                                   Save answer + notify
                                                                                   (successive writes)
                           turn.completed <---------------------------------------+  |
                                                      turn.subagent.result <---------+
                                                      conversation.poke [child] <-- Acknowledge action

                                                      turn.subagent.result -------> Read child answer
                                                                                   All results ready:
                                                                                   save result + resume
                                                                                   (successive writes)
turn.tool.result <-----------------------------------------------------------------+  |
                                                      turn.agent.resume <------------+
                                                      turn.subagent.result <------- Acknowledge action

                                                      turn.agent.resume ----------> Resume parent agent
turn.completed <------------------------------------------------------------------ Save final answer
                                                      turn.agent.resume <---------- Acknowledge action
```

The parent prompt is recorded as `turn.queued`, and a `conversation.poke`
action is appended to dispatch in a following write. A worker runs
the parent agent. When the model requests tools, Fallingwater records one
`turn.tool.requested` event containing the full request set, message delta,
and usage. Following writes dispatch one `turn.tool.call` per call.

A subagent request starts as an ordinary deferred tool call. When a worker
executes the tool and receives a `AgentRequest` for a new, immediately
runnable conversation, it writes four entries in order:

1. Parent: `turn.agent.requested`, containing correlation IDs, the complete
   resolved agent/model/dependency/prompt specification, name, and persistence.
2. Child: `conversation.created`, containing its settings.
3. Child: `turn.queued`, containing its prompt, turn ID, and parent tool reference.
4. Dispatch: `conversation.poke`, targeting the child conversation and turn.

The worker acknowledges the parent tool action and becomes available for
other work. No worker stays blocked waiting for the child. Child IDs are
deterministic for the parent tool call. Registration and initialization use
cursor-checked appends to prevent duplicate creation or queued events.
A recovered tool action uses its saved delegation specification to finish
missing initialization without rerunning the tool. A missing poke for a ready,
not-yet-started turn can also be republished. Dispatch writes remain separate,
so not every crash window is repaired and duplicate hints remain possible.

Another available worker, or the same worker on a later action, runs the child.
The child can use tools and request further subagents through the same flow.
When it completes, its `turn.completed` event and the parent-directed
`turn.subagent.result` dispatch action are separate writes.

The `turn.subagent.result` action contains routing identifiers and the child's
terminal event ID. Its receiving worker reads that event directly and records
the child's answer message
as a `turn.tool.result` in the parent conversation. The child's full message
history and usage stay in the child stream.
Child-outcome transitions validate the saved delegation and normalize the
terminal payload into the parent's result or failure. Terminal transitions emit
a notification intent; the stream checkpoint binds the child's actual Redis
event ID after persistence. Child-result handling does not load the parent agent.

If all calls in the parent's deferred request set now have results, the
worker saving the final result then dispatches `turn.agent.resume`.
Otherwise, the parent waits for the remaining sibling tool or subagent
results. A worker resumes the parent agent with those results, and the parent
can complete or request more tools.

## Persistent named agents

The default `persistent=False` always creates a fresh conversation, even with
a name. With `persistent=True`, the parent registers the explicit request name
or the referenced agent's definition name. Parent replay reconstructs
`Conversation.agent_by_name`; no separate registry key or Deps mutation is used.
The registry survives parent turns and model steps. Names are parent-scoped.

An existing name reuses its conversation and original settings. A request may
omit `agent_path` when it supplies a name and `persistent=True`; unknown names
and explicit conflicting settings fail the requesting turn. Results remain
answer-only rather than returning raw IDs.

Each delegation saves a new parent request and queues a distinct agent turn;
only first creation saves `conversation.created`. Every new turn emits its own
poke and may execute while other turns in that agent conversation are
unfinished. Each turn retains the completed history and usage captured when it
was queued; later answers do not change that snapshot. Callers must wait for an
answer before submitting a follow-up that depends on it. This matches ordinary
conversation behavior rather than enforcing an agent-specific ordering policy.

Parent notification routing comes from each turn's queued event. The
creation-level parent reference remains a fallback for older events. A
persistent registration does not reserve a worker. Automatic
ownership renewal and complete reconciliation of missing dispatch actions
remain outside this implementation.

## Failed child turn

```text
Parent conversation        Child conversation         Shared dispatch              Workers
-------------------        ------------------         ---------------              -------

                                                      conversation.poke [child] ---> Run child agent
                                                                                   Processing fails
                                                                                   Save failure + notify
                                                                                   (successive writes)
                           turn.failed <------------------------------------------+  |
                                                      turn.subagent.result <---------+
                                                      conversation.poke [child] <-- Acknowledge action

                                                      turn.subagent.result -------> Read child failure
turn.failed <--------------------------------------------------------------------- Save parent failure
                                                      turn.subagent.result <------- Acknowledge action
```

A failed child writes `turn.failed` and then dispatches `turn.subagent.result`.
Under the current failure policy, the receiving worker fails
the parent with a `turn.failed` event. It does not save an ordinary tool result
or schedule an agent resume for that failure. Later sibling results are
ignored once the parent has a terminal event. If the parent is itself a child,
its failure also notifies its own parent through dispatch.

## Stream contents and acknowledgements

Dispatch entries are worker actions with text routing fields. Conversation
entries are durable events with a `type` and a compressed JSON `payload`.
For example, `turn.agent.requested` is a parent conversation event, while
`turn.subagent.result` is a dispatch action.

Workers acknowledge each dispatch action after saving its checkpoint.
Acknowledgement clears its pending status but does not delete the entry from
`fw:dispatch`. Waiting for child work is represented by saved conversation
state and future dispatch actions, rather than a worker holding the original
action open.
