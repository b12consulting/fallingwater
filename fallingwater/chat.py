"""Terminal interaction and display for conversation clients."""

from collections.abc import Callable, Iterator
from contextvars import copy_context
from datetime import datetime
from functools import partial
from queue import Queue
from shutil import get_terminal_size
from threading import Event as ThreadEvent, Thread
from time import monotonic
from typing import Protocol

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.patch_stdout import patch_stdout

from .models import (
    AgentRequestedPayload,
    ConversationCreatedPayload,
    ConversationEvent,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import logger


SESSION_PROMPT = "› "  # "▸ "
STATUS_DURATION = 3.0
STATUS_REFRESH_INTERVAL = 0.25
SPINNER_FRAMES = "◆◈◇⬩"
LEFT_OFFSET = " "


class ChatClient(Protocol):
    """Conversation submission and delivery required by the terminal."""

    conversation_id: str

    @property
    def settings(self) -> ConversationCreatedPayload:
        ...

    def send(self, prompt: str) -> str:
        ...

    def read(self, *, ffwd: bool = False) -> Iterator[ConversationEvent]:
        ...

    def acknowledge(self, event: ConversationEvent) -> None:
        ...

    def queued_event_id(self, turn_id: str) -> str:
        ...

    def rewind(self) -> None:
        ...


class Chat:
    """Render conversation events and accept terminal input."""

    def __init__(self, client: ChatClient) -> None:
        self.client = client
        self._turn_timestamps: dict[str, str] = {}
        self._status: tuple[str, float] | None = None
        self._status_app: Application[str] | None = None
        self._active_turns: set[str] = set()

    def _status_toolbar(self) -> FormattedText:
        now = monotonic()
        status = self._status
        message = status[0] if status is not None and now < status[1] else ""
        if self._active_turns:
            frame = SPINNER_FRAMES[
                int(now / STATUS_REFRESH_INTERVAL) % len(SPINNER_FRAMES)
            ]
            message = f"{LEFT_OFFSET}{frame} {message or 'Working...'}"
        if not message:
            return FormattedText([])
        return FormattedText([("", message)])

    def _show_event_status(self, event: ConversationEvent) -> None:
        app = self._status_app
        if app is None:
            return
        payload = event.payload
        if isinstance(payload, (TurnCompletedPayload, TurnFailedPayload)):
            self._active_turns.discard(payload.turn_id)
        elif not isinstance(payload, ConversationCreatedPayload):
            self._active_turns.add(payload.turn_id)
        if isinstance(payload, ConversationCreatedPayload):
            message = f"Agent settings: {payload.agent}"
        elif isinstance(payload, TurnQueuedPayload):
            message = "Turn queued"
        elif isinstance(payload, ToolRequestedPayload):
            names = " / ".join(
                call["tool_name"] for call in payload.requests.get("calls", [])
            )
            message = (
                f"Tools requested: {names}" if names else "Tools requested"
            )
        elif isinstance(payload, ToolResultPayload):
            message = "Tool result received"
        elif isinstance(payload, AgentRequestedPayload):
            message = (
                f"Subagent requested: {payload.name or payload.agent_path}"
            )
        elif isinstance(payload, TurnCompletedPayload):
            message = f"{LEFT_OFFSET}Turn completed"
        else:
            message = f"{LEFT_OFFSET}Turn failed"
        self._status = (
            " ".join(message.split()),
            monotonic() + STATUS_DURATION,
        )
        app.invalidate()

    @staticmethod
    def timestamp(entry_id: str) -> str:
        milliseconds = int(entry_id.split("-", 1)[0])
        timestamp = datetime.fromtimestamp(milliseconds / 1000)
        format = (
            "%H:%M:%S"
            if timestamp.date() == datetime.now().date()
            else "%Y/%m/%d %H:%M:%S"
        )
        return timestamp.strftime(format)

    @staticmethod
    def print_message(
        timestamp: str, message: str, *, queued: bool = False
    ) -> None:
        width = get_terminal_size().columns
        print_formatted_text(f"{timestamp:>{width}}")

        style = ""
        if queued:
            style = "bg:#e5e5e5 #202020"
            message = SESSION_PROMPT + message
        print_formatted_text(FormattedText([(style, message)]))

    def handle_group_event(self, event: ConversationEvent) -> str | None:
        """Render a displayable event and return its completed turn ID."""
        self._show_event_status(event)
        payload = event.payload
        if isinstance(payload, TurnQueuedPayload):
            timestamp = self.timestamp(event.entry_id)
            self._turn_timestamps[payload.turn_id] = timestamp
            self.print_message(timestamp, payload.message, queued=True)
            return None

        if isinstance(payload, (TurnCompletedPayload, TurnFailedPayload)):
            turn_id = payload.turn_id
            if turn_id not in self._turn_timestamps:
                self._turn_timestamps[turn_id] = self.timestamp(
                    self.client.queued_event_id(turn_id)
                )
            if isinstance(payload, TurnCompletedPayload):
                message = payload.message
            else:
                message = f"Turn failed: {payload.error}"
            self.print_message(self._turn_timestamps[turn_id], message)
            return turn_id

        return None

    def read(self, *, ffwd: bool = False) -> Iterator[str]:
        """Yield completed turn IDs, draining available events when ffwd is true."""
        for event in self.client.read(ffwd=ffwd):
            turn_id = self.handle_group_event(event)
            self.client.acknowledge(event)
            if turn_id is not None:
                yield turn_id

    def resume_group(self) -> None:
        """Read available events before accepting another turn."""
        for _ in self.read(ffwd=True):
            pass

    def send_and_wait(self, prompt: str) -> None:
        turn_id = self.client.send(prompt)
        while True:
            completed = False
            for completed_turn_id in self.read():
                if completed_turn_id == turn_id:
                    completed = True
            if completed:
                return

    def run_once(self, prompt: str) -> None:
        """Print the result of one prompt and return without opening a terminal."""
        self.resume_group()
        self.send_and_wait(prompt)

    def run(self) -> None:
        """Accept overlapping prompts while displaying results as they arrive."""
        print(f"Conversation: {self.client.conversation_id}")
        print(f"Model: {self.client.settings.model}")
        print(f"Agent: {self.client.settings.agent}")
        try:
            self.run_terminal()
        except KeyboardInterrupt:
            pass
        print("Bye.")

    def run_terminal(self) -> None:
        stop = ThreadEvent()
        failures: Queue[Exception] = Queue()
        session = PromptSession(
            erase_when_done=True,
            bottom_toolbar=self._status_toolbar,
            refresh_interval=STATUS_REFRESH_INTERVAL,
        )

        session.app.before_render += partial(
            self._check_reader, failures=failures
        )

        self._status_app = session.app
        try:
            with patch_stdout():
                self.resume_group()
                # Keep prompt_toolkit's application session in the reader thread.
                reader = Thread(
                    target=copy_context().run,
                    args=(
                        self._read_replies,
                        stop,
                        failures,
                        session.app.invalidate,
                    ),
                )
                reader.start()
                try:
                    while not stop.is_set():
                        prompt = session.prompt(SESSION_PROMPT)
                        if not stop.is_set():
                            self.client.send(prompt)
                except (EOFError, KeyboardInterrupt):
                    pass
                finally:
                    stop.set()
                    reader.join()
                if not failures.empty():
                    raise failures.get()
        finally:
            self._status_app = None
            self._status = None
            self._active_turns.clear()

    @staticmethod
    def _check_reader(
        app: Application[str], failures: Queue[Exception]
    ) -> None:
        if not failures.empty() and not app.is_done:
            app.exit(exception=EOFError)

    def _read_replies(
        self,
        stop: ThreadEvent,
        failures: Queue[Exception],
        invalidate: Callable[[], None],
    ) -> None:
        try:
            while not stop.is_set():
                for _ in self.read():
                    if stop.is_set():
                        return
        except Exception as error:
            logger.exception(
                "Conversation reader failed: %s", self.client.conversation_id
            )
            failures.put(error)
            stop.set()
            invalidate()
