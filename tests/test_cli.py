"""Smoke checks for the installed CLI entry point."""

from importlib.metadata import version
from unittest.mock import MagicMock

import pytest
from redis import Redis

from fallingwater.cli import _new_cli_chat, build_parser, main


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


def test_chat_positional_conversation_and_short_options() -> None:
    args = build_parser().parse_args(
        ["chat", "boring_wozniak", "-a", "example:agent", "-m", "test"]
    )
    assert args.conversation == "boring_wozniak"
    assert args.agent == "example:agent"
    assert args.model == "test"


def test_chat_argument_defaults_come_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:haiku_master")
    monkeypatch.setenv("FW_MODEL", "openai:gpt-4o-mini")

    args = build_parser().parse_args(["chat"])

    assert args.conversation is None
    assert args.agent == "fallingwater.demo:haiku_master"
    assert args.model == "openai:gpt-4o-mini"

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


def test_chat_rejects_invalid_agent_before_creating_conversation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_from_url = MagicMock()
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis_from_url)

    with pytest.raises(SystemExit, match="cannot instantiate agent"):
        main(["chat", "--agent", "fallingwater.demo:missing_agent"])

    redis_from_url.assert_not_called()


def test_chat_requires_model_if_agent_has_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FW_MODEL", raising=False)
    redis_from_url = MagicMock()
    monkeypatch.setattr("fallingwater.cli.Redis.from_url", redis_from_url)

    with pytest.raises(SystemExit, match="pass --model"):
        main(["chat", "--agent", "fallingwater.demo:haiku_master"])

    redis_from_url.assert_not_called()


def test_chat_accepts_explicit_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:missing_agent")
    monkeypatch.setenv("FW_MODEL", "openai:another-model")
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock()
    new_chat = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli._new_cli_chat", new_chat)

    assert (
        main(
            [
                "chat",
                "--agent",
                "fallingwater.demo:haiku_master",
                "--model",
                "openai:gpt-4o-mini",
            ]
        )
        == 0
    )

    chat.run.assert_called_once_with()
    assert new_chat.call_args.args[2:] == (
        "fallingwater.demo:haiku_master",
        "openai:gpt-4o-mini",
    )


def test_chat_uses_agent_and_model_environment_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FW_AGENT", "fallingwater.demo:haiku_master")
    monkeypatch.setenv("FW_MODEL", "openai:gpt-4o-mini")
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    new_chat = MagicMock()
    monkeypatch.setattr("fallingwater.cli._new_cli_chat", new_chat)

    assert main(["chat"]) == 0

    assert new_chat.call_args.args[2:] == (
        "fallingwater.demo:haiku_master",
        "openai:gpt-4o-mini",
    )


def test_new_cli_chat_retries_existing_name(monkeypatch: pytest.MonkeyPatch) -> None:
    names = iter(["happy_turing", "hopeful_morse"])
    monkeypatch.setattr("fallingwater.cli.generate_name", lambda: next(names))
    redis = MagicMock(spec=Redis)
    redis.exists.side_effect = [True, False]
    chat = MagicMock()
    chat_class = MagicMock(return_value=chat)
    monkeypatch.setattr("fallingwater.cli.Chat", chat_class)

    assert (
        _new_cli_chat(redis, "fw-test", "fallingwater.demo:haiku_master", "test")
        is chat
    )

    assert [call.args[0] for call in redis.exists.call_args_list] == [
        "fw-test:conversation:happy_turing:events",
        "fw-test:conversation:hopeful_morse:events",
    ]
    assert chat_class.call_args.kwargs["conversation_id"] == "hopeful_morse"


def test_chat_resume_uses_saved_values_despite_options_and_environment(
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

    assert "agent_path" not in chat_class.call_args.kwargs
    assert "model" not in chat_class.call_args.kwargs
    chat.run.assert_called_once_with()


def test_chat_checks_saved_agent_on_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )
    chat = MagicMock(agent_path="fallingwater.demo:missing_agent")
    monkeypatch.setattr("fallingwater.cli.Chat", MagicMock(return_value=chat))

    with pytest.raises(SystemExit, match="cannot instantiate agent"):
        main(["chat", "abc"])

    chat.run.assert_not_called()


def test_reset_deletes_only_selected_namespace(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock()
    redis.scan_iter.return_value = iter(
        ["fw:work", "fw-test:work", "fw-test:conversation:abc:events", "fw-tests:work"]
    )
    redis.delete.return_value = 2
    connection = MagicMock()
    connection.__enter__.return_value = redis
    monkeypatch.setattr(
        "fallingwater.cli.Redis.from_url", lambda *args, **kwargs: connection
    )

    assert main(["-n", "fw", "reset", "-n", "fw-test"]) == 0

    redis.delete.assert_called_once_with(
        "fw-test:work", "fw-test:conversation:abc:events"
    )
    assert "Deleted 2 key(s) from namespace 'fw-test'." in capsys.readouterr().out
