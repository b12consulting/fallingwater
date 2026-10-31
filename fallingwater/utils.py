"""
Shared logging for Fallingwater.
"""

import logging
import os

import zstandard as zstd

from fallingwater.catalog import reify_agent


def compress(data: bytes) -> bytes:
    """
    Compress a conversation stream payload with Zstandard.
    """
    return zstd.compress(data)


def decompress(data: bytes) -> bytes:
    """
    Decompress a Zstandard payload from a conversation stream.
    """
    return zstd.decompress(data)


def check_agent(agent_path: str, model: str | None) -> None:
    """
    Validate the effective agent and model before using the chat.
    """
    try:
        agent = reify_agent(agent_path)
    except Exception as error:
        logger.debug(
            "Could not instantiate agent %s", agent_path, exc_info=True
        )
        raise ValueError(
            f"cannot instantiate agent {agent_path!r}: {error}"
        ) from error
    if any((model, agent.model, agent._root_capability.get_model())):
        return
    raise ValueError(
        f"agent {agent_path!r} has no model; "
        "and no model were given"
    )


fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("fallingwater")
logger.setLevel("INFO")
if os.environ.get("FW_DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Log level set to debug")
