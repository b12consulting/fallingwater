"""Terminal interaction and display for Redis-backed conversations."""

from collections.abc import Callable, Iterator
from contextvars import copy_context
from datetime import datetime
from functools import partial
from queue import Queue
from shutil import get_terminal_size
from threading import Event as ThreadEvent, Thread

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.patch_stdout import patch_stdout

from .conversation_client import ConversationClient
from .models import (
    ConversationEvent,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import logger


SESSION_PROMPT = "› "  # "▸ "


class Chat:
    """Render conversation events and accept terminal input."""

    def __init__(self, client: ConversationClient) -> None:
        self.client = client
        self._turn_timestamps: dict[str, str] = {}

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
        turn_id, _ = self.client.send(prompt)
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
                        self.client.send(prompt)
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
            logger.exception(
                "Conversation reader failed: %s", self.client.conversation_id
            )
            failures.put(error)
            stop.set()
            invalidate()
