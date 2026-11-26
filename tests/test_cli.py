"""
Smoke checks for the installed CLI entry point.
"""

from importlib.metadata import version
from unittest.mock import MagicMock, call

import pytest
from prompt_toolkit.application import create_app_session
from redis import Redis

from fallingwater.cli import build_parser, main
from fallingwater.local_client import LocalClient


@pytest.fixture
def client_class(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client_class = MagicMock()
    monkeypatch.setattr("fallingwater.cli.RedisClient", client_class)
    return client_class


def test_cli_help_and_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as help_exit:
        main(["--help"])
    assert help_exit.value.code == 0
    assert "Fallingwater agent tools" in capsys.readouterr().out

    with pytest.raises(SystemExit) as version_exit:
        main(["-V"])
    assert version_exit.value.code == 0
    assert capsys.readouterr().out.strip() == version("fallingwater")

    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == version("fallingwater")


def test_redis_options_are_global() -> None:
    args = build_parser().parse_args(
        [
            "-r",
            "redis://example:6379/1",
            "-n",
            "fw-test",
            "worker",
            "-w",
            "2",
            "--claim-idle-ms",
            "1500",
        ]
    )
    assert args.redis_url == "redis://example:6379/1"
    assert args.namespace == "fw-test"
    assert args.command == "worker"
    assert args.workers == 2
    assert args.claim_idle_ms == 1500


def test_sync_command_copies_positional_namespaces(
    isolated_redis: Redis,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    redis = isolated_redis
    redis.xadd("source:dispatch", {"type": "poke"}, id="1-0")
    connection = MagicMock()
    connection.__enter__.return_value = redis
    from_url = MagicMock(return_value=connection)
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", from_url)

    assert (
        main(
            [
                "-r",
                "redis://example:6379/1",
                "-n",
                "ignored",
                "sync",
                "source",
                "destination",
            ]
        )
        == 0
    )

    from_url.assert_called_once_with("redis://example:6379/1")
    assert redis.xrange("destination:dispatch") == redis.xrange(
        "source:dispatch"
    )
    assert capsys.readouterr().out.strip() == (
        "Copied 1 stream(s) from namespace 'source' to 'destination'."
    )

    with pytest.raises(ValueError, match="already exists"):
        main(["sync", "source", "destination"])


@pytest.mark.parametrize(
    ("command", "function"),
    [("save", "save_namespace"), ("load", "load_namespace")],
)
def test_archive_commands_use_positional_namespace(
    monkeypatch: pytest.MonkeyPatch, command: str, function: str
) -> None:
    redis = MagicMock(spec=Redis)
    connection = MagicMock()
    connection.__enter__.return_value = redis
    from_url = MagicMock(return_value=connection)
    operation = MagicMock(return_value=42)
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", from_url)
    monkeypatch.setattr(f"fallingwater.cli.archive.{function}", operation)

    assert (
        main(
            [
                "-r",
                "redis://example:6379/1",
                "-n",
                "ignored",
                command,
                "selected:",
                "--store",
                "archives",
            ]
        )
        == 0
    )
    from_url.assert_called_once_with("redis://example:6379/1")
    operation.assert_called_once_with(redis, "selected", store="archives")

    operation.side_effect = ValueError("invalid archive")
    with pytest.raises(ValueError, match="invalid archive"):
        main([command, "selected:"])


@pytest.mark.parametrize(
    ("command", "function"),
    [
        ("save", "save_conversation"),
        ("load", "load_conversation"),
    ],
)
def test_archive_commands_use_conversation_in_global_namespace(
    monkeypatch: pytest.MonkeyPatch, command: str, function: str
) -> None:
    redis = MagicMock(spec=Redis)
    connection = MagicMock()
    connection.__enter__.return_value = redis
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *_args: connection
    )
    operation = MagicMock(return_value=42)
    monkeypatch.setattr(f"fallingwater.cli.archive.{function}", operation)
    other_function = "save_namespace" if command == "save" else "load_namespace"
    namespace_operation = MagicMock()
    monkeypatch.setattr(
        f"fallingwater.cli.archive.{other_function}", namespace_operation
    )

    assert main(["-n", "selected", command, "abc", "--store", "archives"]) == 0
    operation.assert_called_once_with(
        redis, "selected", "abc", store="archives"
    )
    namespace_operation.assert_not_called()
    operation.side_effect = ValueError("invalid archive")
    with pytest.raises(ValueError, match="invalid archive"):
        main([command, "abc"])


@pytest.mark.parametrize("command", ["save", "load"])
@pytest.mark.parametrize("target", [":", "fw::"])
def test_archive_commands_reject_invalid_namespace_targets(
    monkeypatch: pytest.MonkeyPatch, command: str, target: str
) -> None:
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *_args: connection
    )
    with pytest.raises(ValueError, match="namespace"):
        main([command, target])


def test_web_command_passes_config_to_app_and_uvicorn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("jinja2")
    pytest.importorskip("uvicorn")
    app = object()
    redis = MagicMock(spec=Redis)
    redis_context = MagicMock()
    redis_context.__enter__.return_value = redis
    create_app = MagicMock(return_value=app)
    serve = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", MagicMock(return_value=redis_context)
    )
    monkeypatch.setattr("fallingwater.web.apirouter.create_app", create_app)
    monkeypatch.setattr("uvicorn.run", serve)

    assert (
        main(
            [
                "-r",
                "redis://example:6379/1",
                "-n",
                "fw-test",
                "web",
                "--host",
                "0.0.0.0",
                "--port",
                "9000",
            ]
        )
        == 0
    )

    create_app.assert_called_once_with(namespace="fw-test", redis=redis)
    serve.assert_called_once_with(
        app,
        host="0.0.0.0",
        port=9000,
        timeout_graceful_shutdown=3,
    )


def test_chat_positional_conversation_and_short_options() -> None:
    args = build_parser().parse_args(
        [
            "chat",
            "boring_wozniak",
            "-a",
            "example:agent",
            "-m",
            "test",
            "--group",
            "web-user",
            "-p",
            "name=Alice",
            "--param",
            "message=hello=world",
        ]
    )
    assert args.conversation == "boring_wozniak"
    assert args.params == ["name=Alice", "message=hello=world"]
    assert args.agent == "example:agent"
    assert args.model == "test"
    assert args.group == "web-user"


def test_chat_argument_defaults_come_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:haiku_master")
    monkeypatch.setenv("FW_MODEL", "openai:gpt-4o-mini")

    args = build_parser().parse_args(["chat"])

    assert args.conversation is None
    assert args.agent == "fallingwater.demo:haiku_master"
    assert args.model == "openai:gpt-4o-mini"
    assert args.group == "user"
    assert not args.local
    assert not args.rewind
    assert args.msg is None
    assert args.params == []

    explicit = build_parser().parse_args(
        [
            "chat",
            "--agent",
            "fallingwater.demo:haiku_master",
            "--model",
            "openai:gpt-4o-mini",
        ]
    )
    assert explicit.agent == "fallingwater.demo:haiku_master"
    assert explicit.model == "openai:gpt-4o-mini"


def test_chat_uses_plain_pydantic_agent_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FW_AGENT", raising=False)

    args = build_parser().parse_args(["chat"])

    assert args.agent == "pydantic_ai:Agent"


def test_named_chat_passes_agent_and_model_to_client(
    monkeypatch: pytest.MonkeyPatch, client_class: MagicMock
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:missing_agent")
    monkeypatch.setenv("FW_MODEL", "openai:another-model")
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock()
    chat_class = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert (
        main(
            [
                "chat",
                "test",
                "--agent",
                "fallingwater.demo:haiku_master",
                "--model",
                "openai:gpt-4o-mini",
                "--group",
                "reader-one",
                "-p",
                "name=Alice",
                "--param",
                "region=eu-west",
            ]
        )
        == 0
    )

    chat.run.assert_called_once_with()
    client_class.return_value.rewind.assert_not_called()
    chat_class.assert_called_once_with(client_class.return_value)
    assert client_class.call_args.kwargs == {
        "namespace": "fw",
        "conversation_id": "test",
        "agent_path": "fallingwater.demo:haiku_master",
        "model": "openai:gpt-4o-mini",
        "group": "reader-one",
        "deps": {"name": "Alice", "region": "eu-west"},
    }


def test_chat_uses_agent_and_model_environment_defaults(
    monkeypatch: pytest.MonkeyPatch, client_class: MagicMock
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:haiku_master")
    monkeypatch.setenv("FW_MODEL", "openai:gpt-4o-mini")
    connection = MagicMock()
    connection.__enter__.return_value.exists.return_value = False
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    monkeypatch.setattr(
        "fallingwater.cli.generate_name", lambda: "bright_turing"
    )
    chat_class = MagicMock()
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert main(["chat"]) == 0

    chat_class.assert_called_once_with(client_class.return_value)
    assert client_class.call_args.kwargs == {
        "namespace": "fw",
        "conversation_id": "bright_turing",
        "agent_path": "fallingwater.demo:haiku_master",
        "model": "openai:gpt-4o-mini",
        "group": "user",
        "deps": {},
    }


def test_cli_chat_retries_existing_generated_name(
    monkeypatch: pytest.MonkeyPatch, client_class: MagicMock
) -> None:
    names = iter(["happy_turing", "hopeful_morse"])
    monkeypatch.setattr("fallingwater.cli.generate_name", lambda: next(names))
    redis = MagicMock(spec=Redis)
    redis.exists.side_effect = [True, False]
    connection = MagicMock()
    connection.__enter__.return_value = redis
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock()
    chat_class = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert main(["-n", "fw-test", "chat"]) == 0

    assert [call.args[0] for call in redis.exists.call_args_list] == [
        "fw-test:conversation:happy_turing:events",
        "fw-test:conversation:hopeful_morse:events",
    ]
    assert client_class.call_args.kwargs["conversation_id"] == "hopeful_morse"


def test_cli_passes_resume_options_to_client(
    monkeypatch: pytest.MonkeyPatch, client_class: MagicMock
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:missing_agent")
    monkeypatch.setenv("FW_MODEL", "openai:another-model")
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock(
        agent_path="fallingwater.demo:haiku_master", model="openai:gpt-4o-mini"
    )
    chat_class = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert (
        main(
            [
                "chat",
                "abc",
                "--agent",
                "fallingwater.demo:missing_agent",
                "--model",
                "openai:another-model",
            ]
        )
        == 0
    )

    assert (
        client_class.call_args.kwargs["agent_path"]
        == "fallingwater.demo:missing_agent"
    )
    assert client_class.call_args.kwargs["model"] == "openai:another-model"
    chat.run.assert_called_once_with()


def test_chat_msg_sends_one_prompt_without_starting_terminal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    client_class: MagicMock,
) -> None:
    connection = MagicMock()
    connection.__enter__.return_value.exists.return_value = False
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    monkeypatch.setattr(
        "fallingwater.cli.generate_name", lambda: "bright_turing"
    )
    chat = MagicMock(conversation_id="bright_turing")
    client_class.return_value.conversation_id = "bright_turing"
    monkeypatch.setattr("fallingwater.cli.Chat", MagicMock(return_value=chat))

    assert main(["chat", "--msg", "Hello"]) == 0

    chat.run_once.assert_called_once_with("Hello")
    chat.run.assert_not_called()
    assert capsys.readouterr().err.strip() == "Conversation: bright_turing"


@pytest.mark.parametrize("message", [None, "Hello"])
def test_chat_rewinds_selected_group_before_reading(
    monkeypatch: pytest.MonkeyPatch,
    message: str | None,
    client_class: MagicMock,
) -> None:
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock()
    chat_class = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)
    operations = MagicMock()
    operations.attach_mock(client_class.return_value.rewind, "rewind")
    operations.attach_mock(chat.run, "run")
    operations.attach_mock(chat.run_once, "run_once")
    args = ["chat", "abc", "--group", "reader-one", "--rewind"]
    if message is not None:
        args.extend(["--msg", message])

    assert main(args) == 0

    assert client_class.call_args.kwargs["group"] == "reader-one"
    assert operations.method_calls == [
        call.rewind(),
        call.run() if message is None else call.run_once(message),
    ]


@pytest.mark.parametrize("message", [None, "Hello"])
def test_local_chat_selects_client_and_cleans_up_without_redis(
    monkeypatch, message, client_class
) -> None:
    client = MagicMock()
    client.__enter__.return_value = client
    local_class = MagicMock(return_value=client)
    redis = MagicMock(side_effect=AssertionError("Local chat opened Redis"))
    chat = MagicMock()
    monkeypatch.setattr("fallingwater.cli.LocalClient", local_class)
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis)
    monkeypatch.setattr("fallingwater.cli.Chat", MagicMock(return_value=chat))
    args = [
        "chat",
        "local-session",
        "--local",
        "--agent",
        "example:agent",
        "--model",
        "test",
        "--group",
        "reader-one",
        "--rewind",
        "-p",
        "name=Alice",
    ]
    if message is not None:
        args.extend(["--msg", message])

    assert main(args) == 0

    local_class.assert_called_once_with(
        conversation_id="local-session",
        agent_path="example:agent",
        model="test",
        group="reader-one",
        deps={"name": "Alice"},
        load=False,
        store=".fw-store",
    )
    client.rewind.assert_called_once_with()
    if message is None:
        chat.run.assert_called_once_with()
        chat.run_once.assert_not_called()
    else:
        chat.run_once.assert_called_once_with(message)
        chat.run.assert_not_called()
    client.__exit__.assert_called_once()
    redis.assert_not_called()
    client_class.assert_not_called()


def test_local_archive_cli_resumes_without_reprinting_history(
    tmp_path, monkeypatch, capsys
) -> None:
    redis = MagicMock(side_effect=AssertionError("Local chat opened Redis"))
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis)
    args = [
        "chat",
        "saved",
        "--local",
        "--store",
        str(tmp_path),
        "--save",
    ]
    with create_app_session():
        assert (
            main(
                args
                + [
                    "--agent",
                    "tests.test_local_client:writer",
                    "--msg",
                    "first",
                ]
            )
            == 0
        )
        capsys.readouterr()
        assert main(args + ["--load", "--msg", "second"]) == 0
        output = capsys.readouterr().out
        assert "first/second" in output
        assert "› first" not in output
        assert main(args + ["--load", "--rewind", "--msg", "third"]) == 0
        output = capsys.readouterr().out
        assert "first/second/third" in output
        assert "› first" in output
    redis.assert_not_called()


@pytest.mark.parametrize(
    ("args", "error"),
    [
        (["chat", "--load"], "require --local"),
        (["chat", "--save"], "require --local"),
        (["chat", "--local", "--load"], "requires a conversation ID"),
    ],
)
def test_local_archive_cli_rejects_invalid_options(args, error) -> None:
    with pytest.raises(SystemExit, match=error):
        main(args)


def test_local_archive_cli_saves_after_exception(tmp_path, monkeypatch) -> None:
    def fail_chat(chat):
        chat.client.ask("recorded")
        raise RuntimeError("terminal failed")

    monkeypatch.setattr("fallingwater.cli.Chat.run", fail_chat)
    with pytest.raises(RuntimeError, match="terminal failed"):
        main(
            [
                "chat",
                "saved",
                "--local",
                "--save",
                "--store",
                str(tmp_path),
                "--agent",
                "tests.test_local_client:writer",
            ]
        )
    with LocalClient(
        conversation_id="saved", load=True, store=str(tmp_path)
    ) as client:
        assert client.ask("next") == "recorded/next"


def test_local_archive_cli_saves_unfinished_turn_without_draining(
    tmp_path, monkeypatch
) -> None:
    def stop_chat(chat):
        chat.client.executor.send(chat.client.conversation_id, "unfinished")

    monkeypatch.setattr("fallingwater.cli.Chat.run", stop_chat)
    assert (
        main(
            [
                "chat",
                "partial",
                "--local",
                "--save",
                "--store",
                str(tmp_path),
                "--agent",
                "tests.test_local_client:writer",
            ]
        )
        == 0
    )
    with pytest.raises(ValueError, match="unfinished turns"):
        LocalClient(conversation_id="partial", load=True, store=str(tmp_path))


def test_local_chat_executes_message_and_closes_executor(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr("fallingwater.cli.generate_name", lambda: "local-test")
    redis = MagicMock(side_effect=AssertionError("Local chat opened Redis"))
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis)
    clients = []

    def open_client(**kwargs):
        client = LocalClient(**kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr("fallingwater.cli.LocalClient", open_client)
    with create_app_session():
        assert (
            main(
                [
                    "chat",
                    "--local",
                    "--agent",
                    "pydantic_ai:Agent",
                    "--model",
                    "test",
                    "--msg",
                    "Hello",
                ]
            )
            == 0
        )

    client = clients[0]
    output = capsys.readouterr()
    assert "› Hello" in output.out
    assert "success (no tool calls)" in output.out
    assert output.err.strip() == "Conversation: local-test"
    assert client._thread is not None
    assert not client._thread.is_alive()
    assert all(
        turn.completed for turn in client.conversation.turn_state.values()
    )
    redis.assert_not_called()


def test_local_chat_closes_client_when_terminal_fails(monkeypatch) -> None:
    clients = []

    def open_client(**kwargs):
        client = LocalClient(**kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr("fallingwater.cli.LocalClient", open_client)
    chat = MagicMock()
    chat.run.side_effect = RuntimeError("terminal failed")
    monkeypatch.setattr("fallingwater.cli.Chat", lambda _client: chat)
    with pytest.raises(RuntimeError, match="terminal failed"):
        main(
            [
                "chat",
                "--local",
                "--agent",
                "pydantic_ai:Agent",
                "--model",
                "test",
            ]
        )

    with pytest.raises(RuntimeError, match="Local executor is closed"):
        clients[0].send("after exit")
    assert clients[0].executor.closed


def test_local_chat_reports_invalid_configuration_without_redis(
    monkeypatch,
) -> None:
    redis = MagicMock(side_effect=AssertionError("Local chat opened Redis"))
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis)

    with pytest.raises(SystemExit, match="fw chat: conversation reader group"):
        main(["chat", "--local", "--model", "test", "--group", ""])

    redis.assert_not_called()


def test_reset_deletes_only_selected_namespace(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock()
    redis.scan_iter.return_value = iter(
        [
            "fw:dispatch",
            "fw-test:dispatch",
            "fw-test:conversation:abc:events",
            "fw-tests:dispatch",
        ]
    )
    redis.delete.return_value = 2
    connection = MagicMock()
    connection.__enter__.return_value = redis
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )

    assert main(["-n", "fw", "reset", "-n", "fw-test"]) == 0

    redis.delete.assert_called_once_with(
        "fw-test:dispatch", "fw-test:conversation:abc:events"
    )
    assert (
        "Deleted 2 key(s) from namespace 'fw-test'." in capsys.readouterr().out
    )
