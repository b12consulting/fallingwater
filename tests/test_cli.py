"""Smoke checks for the installed CLI entry point."""

from importlib.metadata import version
from unittest.mock import MagicMock

import pytest
from redis import Redis

from fallingwater.cli import build_parser, main


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
        ["-r", "redis://example:6379/1", "-n", "fw-test", "worker", "-p", "2"]
    )
    assert args.redis_url == "redis://example:6379/1"
    assert args.namespace == "fw-test"
    assert args.command == "worker"
    assert args.proxies == 2


def test_web_command_passes_config_to_app_and_uvicorn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")
    app = object()
    create_app = MagicMock(return_value=app)
    serve = MagicMock()
    monkeypatch.setattr("fallingwater.web.create_app", create_app)
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

    create_app.assert_called_once_with(
        redis_url="redis://example:6379/1", namespace="fw-test"
    )
    serve.assert_called_once_with(app, host="0.0.0.0", port=9000)


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
        ]
    )
    assert args.conversation == "boring_wozniak"
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


def test_named_chat_passes_agent_and_model_to_chat(
    monkeypatch: pytest.MonkeyPatch,
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
            ]
        )
        == 0
    )

    chat.run.assert_called_once_with()
    assert chat_class.call_args.kwargs == {
        "namespace": "fw",
        "conversation_id": "test",
        "agent_path": "fallingwater.demo:haiku_master",
        "model": "openai:gpt-4o-mini",
        "group": "reader-one",
    }


def test_chat_uses_agent_and_model_environment_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:haiku_master")
    monkeypatch.setenv("FW_MODEL", "openai:gpt-4o-mini")
    connection = MagicMock()
    connection.__enter__.return_value.exists.return_value = False
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    monkeypatch.setattr("fallingwater.cli.generate_name", lambda: "bright_turing")
    chat_class = MagicMock()
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert main(["chat"]) == 0

    assert chat_class.call_args.kwargs == {
        "namespace": "fw",
        "conversation_id": "bright_turing",
        "agent_path": "fallingwater.demo:haiku_master",
        "model": "openai:gpt-4o-mini",
        "group": "user",
    }


def test_cli_chat_retries_existing_generated_name(
    monkeypatch: pytest.MonkeyPatch,
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
    assert chat_class.call_args.kwargs["conversation_id"] == "hopeful_morse"


def test_cli_passes_resume_options_to_chat(
    monkeypatch: pytest.MonkeyPatch,
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
        chat_class.call_args.kwargs["agent_path"] == "fallingwater.demo:missing_agent"
    )
    assert chat_class.call_args.kwargs["model"] == "openai:another-model"
    chat.run.assert_called_once_with()


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
    assert "Deleted 2 key(s) from namespace 'fw-test'." in capsys.readouterr().out
