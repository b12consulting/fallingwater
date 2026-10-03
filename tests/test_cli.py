"""Smoke checks for the installed CLI entry point."""

from importlib.metadata import version

import pytest

from fallingwater.cli import main


def test_cli_help_and_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as help_exit:
        main(["--help"])
    assert help_exit.value.code == 0
    assert "Fallingwater agent tools" in capsys.readouterr().out

    with pytest.raises(SystemExit) as version_exit:
        main(["--version"])
    assert version_exit.value.code == 0
    assert capsys.readouterr().out.strip() == version("fallingwater")

    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == version("fallingwater")
