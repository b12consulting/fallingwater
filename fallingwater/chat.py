"""Terminal and library workflows for Redis-backed conversations."""

from collections.abc import Callable, Iterator, Mapping
from contextvars import copy_context
from datetime import datetime
from functools import partial
from queue import Queue
from shutil import get_terminal_size
from threading import Event as ThreadEvent, Thread
from typing import cast
from uuid import uuid4

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.patch_stdout import patch_stdout
from redis import Redis

from .stream import DispatchStream
from .conversation import Conversation
from .models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import logger, check_agent


SESSION_PROMPT = "› " # "▸ "


class Chat:
    """Submit turns and observe their results through Redis."""

    consumer = "terminal"

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        conversation_id: str | None = None,
        agent_path: str | None = None,
        model: str | None = None,
        group: str = "user",
        deps: Mapping[str, str]
        | None = None,  # FIXME use deps class from py-ai
    ) -> None:
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self.cursor = "0"
        self._turn_timestamps: dict[str, str] = {}
        self.conversation_id = conversation_id or str(uuid4())
        self.stream = DispatchStream(
            redis, namespace=namespace
        ).conversation_stream(self.conversation_id)
        self.events = self.stream.name

        records = self.stream.read_conversation_range(count=1)
        record = records[0] if records else None
        if record:
            if record.type != Event.CONVERSATION_CREATED:
                raise ValueError(
                    f"{self.events} is not a Fallingwater conversation"
                )
            if deps:
                raise ValueError(
                    "dependencies can only be set for a new conversation"
                )
            created = cast(ConversationCreatedPayload, record.payload)
            self.agent_path = created.agent
            self.model = created.model
        else:
            if agent_path is None:
                raise ValueError("agent is required for a new conversation")
            self.agent_path = agent_path
            self.model = model or ""

        check_agent(self.agent_path, self.model)
        if not record:
            created = ConversationCreatedPayload(
                agent=self.agent_path,
                model=self.model,
                deps=dict(deps or {}),
            )
            self.stream.append_payload(created)
        self.conversation = Conversation(
            conversation_id=self.conversation_id, settings=created
        )
        self.stream.create_group(self.group, id="0")

    def send(self, prompt: str) -> tuple[str, str]:
        """Queue a prompt as a new turn and return its ID and event cursor."""
        turn_id = str(uuid4())
        logger.debug("SEND (stream: %s): %.20s", self.events, prompt)
        prepared = self.conversation.prepare(
            TurnQueuedPayload(turn_id=turn_id, message=prompt)
        )
        queued_id = self.stream.append_payload(prepared.payload)
        for action in prepared.dispatch(queued_id):
            self.stream.dispatch.append_payload(action)
        return turn_id, queued_id

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            entries = self.stream.read_conversation(cursor, block=1000)
            for event in entries:
                cursor = event.entry_id
                if not isinstance(
                    event.payload,
                    (
                        TurnQueuedPayload,
                        TurnCompletedPayload,
                        TurnFailedPayload,
                    ),
                ):
                    continue
                if event.payload.turn_id != turn_id:
                    continue
                if event.type == Event.TURN_COMPLETED:
                    completed = cast(TurnCompletedPayload, event.payload)
                    return completed.message
                if event.type == Event.TURN_FAILED:
                    failed = cast(TurnFailedPayload, event.payload)
                    raise RuntimeError(failed.error)

    def ask(self, prompt: str) -> str:
        """Submit a new turn and wait for its result."""
        return self.receive(*self.send(prompt))

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
        event_type = event.type
        payload = event.payload
        turn_id = getattr(payload, "turn_id", None)
        if event_type == Event.TURN_QUEUED and turn_id is not None:
            queued = cast(TurnQueuedPayload, payload)
            timestamp = self.timestamp(event.entry_id)
            self._turn_timestamps[turn_id] = timestamp
            self.print_message(
                timestamp, queued.message, queued=True
            )
            self.stream.acknowledge(self.group, event.entry_id)
            return None

        if event_type in (Event.TURN_COMPLETED, Event.TURN_FAILED):
            if turn_id not in self._turn_timestamps:
                for record in self.stream.iter_events():
                    if isinstance(record.payload, TurnQueuedPayload):
                        self._turn_timestamps[
                            record.payload.turn_id
                        ] = self.timestamp(record.entry_id)
            if event_type == Event.TURN_COMPLETED:
                completed = cast(TurnCompletedPayload, payload)
                message = completed.message
            else:
                failed = cast(TurnFailedPayload, payload)
                message = f"Turn failed: {failed.error}"
            self.print_message(
                self._turn_timestamps[turn_id], message
            )
            self.stream.acknowledge(self.group, event.entry_id)
            return turn_id

        self.stream.acknowledge(self.group, event.entry_id)
        return None

    def read(self, *, ffwd: bool = False) -> Iterator[str]:
        """Yield completed turn IDs, draining available events when ffwd is true."""
        logger.debug("READ %s", self.events)
        completed_pending = False
        block = 1000
        while True:
            # Consume Stream
            pending = self.cursor != ">"
            entries = self.stream.read_group(
                self.group,
                self.consumer,
                self.cursor,
                count=100,
                block=None if pending or ffwd else block,
            )
            # handle empty result
            if not entries:
                if not pending:
                    # Pre-existing entries are drained
                    return
                if not ffwd and completed_pending:
                    return
                self.cursor = ">"
                continue

            # process events
            for event in entries:
                turn_id = self.handle_group_event(event)
                if pending:
                    self.cursor = event.entry_id
                if turn_id is not None:
                    completed_pending = True
                    yield turn_id
            if not ffwd and not pending:
                return

    def resume_group(self) -> None:
        """Read available events before accepting another turn."""
        for _ in self.read(ffwd=True):
            pass

    def rewind(self) -> None:
        """Replay retained events from the beginning for this reader group."""
        self.stream.rewind_group(self.group)
        self.cursor = ">"

    def send_and_wait(self, prompt: str) -> None:
        turn_id, _ = self.send(prompt)
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
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        try:
            self.run_terminal()
        except KeyboardInterrupt:
            pass
        print("Bye.")

    def run_terminal(self) -> None:
        stop = ThreadEvent()
        failures: Queue[Exception] = Queue()
        session = PromptSession(erase_when_done=True)

        session.app.before_render += partial(
            self._check_reader, failures=failures
        )

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
                        self.send(prompt)
            except (EOFError, KeyboardInterrupt):
                pass
            finally:
                stop.set()
                reader.join()
            if not failures.empty():
                raise failures.get()

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
            logger.exception("Conversation reader failed: %s", self.events)
            failures.put(error)
            stop.set()
            invalidate()
