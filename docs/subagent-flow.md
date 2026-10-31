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
                                                                                   Get SubAgentRequest
                                                                                   Create child work
                                                                                   (successive writes)
turn.subagent.requested <----------------------------------------------------------+  |
                           conversation.created <------------------------------------+
                           turn.queued <---------------------------------------------+
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
executes the tool and receives a `SubAgentRequest`, it writes four entries
in order:

1. Parent: `turn.subagent.requested`, containing the child conversation and
   turn IDs, together with the parent turn, step, and tool-call IDs.
2. Child: `conversation.created`, containing its agent, model, dependencies,
   and a reference back to the parent tool call.
3. Child: `turn.queued`, containing its prompt and turn ID.
4. Dispatch: `conversation.poke`, targeting the child conversation and turn.

The worker acknowledges the parent tool action and becomes available for
other work. No worker stays blocked waiting for the child. Child IDs are
deterministic for the parent tool call. A recovered action skips a saved
subagent request, but concurrent workers can still create duplicate child work.

Another available worker, or the same worker on a later action, runs the child.
The child can use tools and request further subagents through the same flow.
When it completes, its `turn.completed` event and the parent-directed
`turn.subagent.result` dispatch action are separate writes.

The `turn.subagent.result` action contains routing identifiers. Its receiving
worker reads the child's terminal event and records the child's answer message
as a `turn.tool.result` in the parent conversation. The child's full message
history and usage stay in the child stream.

If all calls in the parent's deferred request set now have results, the
worker saving the final result then dispatches `turn.agent.resume`.
Otherwise, the parent waits for the remaining sibling tool or subagent
results. A worker resumes the parent agent with those results, and the parent
can complete or request more tools.

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
For example, `turn.subagent.requested` is a parent conversation event, while
`turn.subagent.result` is a dispatch action.

Workers acknowledge each dispatch action after saving its checkpoint.
Acknowledgement clears its pending status but does not delete the entry from
`fw:dispatch`. Waiting for child work is represented by saved conversation
state and future dispatch actions, rather than a worker holding the original
action open.
