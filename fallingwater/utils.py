"""Shared logging for Fallingwater."""

import logging
import os


fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("fallingwater")
logger.setLevel("INFO")
if os.environ.get("FW_DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Log level set to debug")
