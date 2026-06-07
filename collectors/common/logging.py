from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from .env import logs_root


def setup_logging(name: str) -> logging.Logger:
    root = logs_root()
    root.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    file_handler = RotatingFileHandler(
        root / f"{name}.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger

