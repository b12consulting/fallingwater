"""I/O-free dashboard presentation and typed template data."""

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, TypedDict
from urllib.parse import quote

from fastapi import Request
from starlette.datastructures import URL

from ..models import ConversationCreatedPayload, ConversationEvent, Event


EVENT_LABELS = {
    Event.CONVERSATION_CREATED: "Conversation created",
    Event.TURN_QUEUED: "User message",
    Event.TURN_TOOL_REQUESTED: "Tools requested",
    Event.TURN_TOOL_RESULT: "Tool result",
    Event.TURN_SUBAGENT_REQUESTED: "Subagent requested",
    Event.TURN_COMPLETED: "Assistant message",
    Event.TURN_FAILED: "Turn failed",
}
EVENT_ICONS = {
    Event.CONVERSATION_CREATED: "file-check",
    Event.TURN_QUEUED: "person",
    Event.TURN_COMPLETED: "robot",
    Event.TURN_SUBAGENT_REQUESTED: "arrow-bar-down",
    Event.TURN_TOOL_REQUESTED: "nut",
    Event.TURN_TOOL_RESULT: "nut-fill",
    Event.TURN_FAILED: "exclamation-square",
}


class DashboardSlot(TypedDict):
    entry_id: str
    label: str
    icon: str | None
    failed: bool
    time: datetime
    details_url: URL


class DashboardCard(TypedDict):
    conversation_id: str
    url: URL
    id_key: str
    settings: ConversationCreatedPayload | None
    failed_count: int
    events: list[DashboardSlot]


class DashboardContext(TypedDict):
    conversations: list[DashboardCard]
    event_limit: int
    before: str
    cutoff_time: datetime | None
    selected_date: str
    prev_id: str


class HistoryContext(TypedDict):
    conversation: DashboardCard
    full_history: bool
    cards_target: str
    refresh_label: str
    events_url: URL


class EventDetails(TypedDict):
    conversation_id: str
    event: ConversationEvent
    label: str
    time: datetime
    failed: bool
    payload: dict[str, Any]


def event_time(entry_id: str) -> datetime:
    return datetime.fromtimestamp(
        int(entry_id.split("-", 1)[0]) / 1000, tz=timezone.utc
    )


def dashboard_card(
    request: Request,
    conversation_id: str,
    events: list[ConversationEvent],
    first: ConversationEvent | None,
) -> DashboardCard:
    return {
        "conversation_id": conversation_id,
        "url": request.url_for(
            "conversation_dashboard",
            conversation_id=quote(conversation_id, safe=""),
        ),
        "id_key": sha256(conversation_id.encode()).hexdigest(),
        "settings": (
            first.payload
            if first and isinstance(first.payload, ConversationCreatedPayload)
            else None
        ),
        "failed_count": sum(
            event.type == Event.TURN_FAILED for event in events
        ),
        "events": [
            {
                "entry_id": event.entry_id,
                "label": EVENT_LABELS[event.type],
                "icon": EVENT_ICONS.get(event.type),
                "failed": event.type == Event.TURN_FAILED,
                "time": event_time(event.entry_id),
                "details_url": request.url_for(
                    "dashboard_event"
                ).include_query_params(
                    conversation_id=conversation_id,
                    entry_id=event.entry_id,
                ),
            }
            for event in events
        ],
    }


def history_context(
    request: Request, conversation_id: str, events: list[ConversationEvent]
) -> HistoryContext:
    return {
        "conversation": dashboard_card(
            request, conversation_id, events, events[0] if events else None
        ),
        "full_history": True,
        "cards_target": "conversation-history",
        "refresh_label": "Refresh history",
        "events_url": request.url_for(
            "events", conversation_id=quote(conversation_id, safe="")
        ),
    }


def event_details(
    conversation_id: str, event: ConversationEvent
) -> EventDetails:
    return {
        "conversation_id": conversation_id,
        "event": event,
        "label": EVENT_LABELS[event.type],
        "time": event_time(event.entry_id),
        "failed": event.type == Event.TURN_FAILED,
        "payload": event.payload.fmt(full=True),
    }
