"""
Shared logging for Fallingwater.
"""

import logging
import os

import brotli


def compress(data: bytes) -> bytes:
    """
    Compress data with Brotli quality 5 for conversation stream payloads.
    """
    return brotli.compress(data, quality=5)


def decompress(data: bytes) -> bytes:
    """
    Decompress a Brotli payload from a conversation stream.
    """
    return brotli.decompress(data)


fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("fallingwater")
logger.setLevel("INFO")
if os.environ.get("FW_DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Log level set to debug")
