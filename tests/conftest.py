"""Configure runtime type checks for tests."""

from typeguard import install_import_hook

install_import_hook("fallingwater")
