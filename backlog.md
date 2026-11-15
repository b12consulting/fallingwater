# Open

## B1. Repeatable conversation settings

Rename `conversation.created` to `conversation.settings` and rename its payload
and enum accordingly. Repeatable settings are implemented under the existing
names: replay replaces the latest settings without resetting conversation
state. An empty conversation is valid; the first event in a nonempty
conversation must still be a settings event.

## B3. Agent definition versioning

Decide whether resumed conversations keep their original agent definition or
adopt changes after an upgrade. Saved import paths currently have no version.

## B4. Change a conversation's model

Provide a user-facing way to change a conversation's model when its saved model
becomes unavailable. Replayed settings now take effect at each execution step,
including already queued turns; models are not pinned per turn. An invocation
already in progress is unchanged. Relates to repeatable settings.

## B5. Terminal answer streaming and progress

Show incremental answers and live tool or delegated-agent progress, beyond
final answers and stream inspection.

## B6. Conversation and dispatch retention

Decide how long conversation events and acknowledged dispatch entries remain
available. Preserve the history and pending work needed for replay and recovery.

## B7. Repair partial writes

Implement reconciliation for saved events missing their next dispatch action.
`recover()` is still a placeholder; replaying saved delegations repairs child
setup, but pending-action reclamation alone does not repair every interrupted
write sequence. Accepted turns must survive worker crashes and restarts.

## B8. Conversation ending

Define what ends a conversation, whether it can end automatically, and what
happens to later submissions and retained data.

## B9. Health and statistics endpoints

Add endpoints for operational health and statistics to the web integration.

## B10. SSE reconnection from a cursor

Define and document cursor-based reconnection so clients resume after their last
event without replaying already received events or consuming worker work.

## B11. Complete stream-class adoption

Move web dispatch discovery, the recent feed's multiplexed `XREAD`, and CLI event
inspection onto the stream classes. Keep workflow coordination outside them.

## B12. Consumer-group lifecycle management

Remove worker consumers with no pending entries when `WorkerPool` starts,
preserving consumers that still own pending work.

## B13. Renew active dispatch ownership

Prevent `XAUTOCLAIM` from reassigning healthy, long-running actions while
preserving recovery of abandoned work. Renew independently of blocking model or
tool execution, from delivery or claim through acknowledgement, and never take
ownership back from another worker. Define renewal failure and ownership-loss
handling, including protection against stale writes and acknowledgements.
General checkpoints currently lack cursor checks, and result persistence and
dispatch publication remain separate writes; renewal is not an exactly-once
external-effects guarantee. See RAID R1, R5, and D89.

## B14. Tool and network timeouts

Define timeouts and bounded retries separately from ownership renewal, which
could otherwise keep a hung call owned indefinitely.

## B15. Real-Redis restart and recovery coverage

Add end-to-end worker restart and partial-write recovery tests, plus coverage for
long-running actions, stopped renewal, ownership loss, and active-action
reclamation. Existing isolated tests cover result fan-in, concurrent persistent
registration, and reader-group rewind, not these full recovery scenarios.

# Closed

## B2. Agent catalog, composition, and discovery

**Status:** Won't do.

Define file-based agent specifications and application-level composition of
agents, tools, and capabilities. Add discovery commands; `fw ls agent` and
`fw ls capability` are proposals, not existing commands.
