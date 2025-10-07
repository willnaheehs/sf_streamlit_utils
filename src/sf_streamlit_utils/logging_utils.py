"""Utilities for logging and measuring metrics.

This module centralises logging so that all components of the package emit
messages through the same logger.  It also provides a simple context
manager for measuring the duration of operations.  By default the logger
logs at the INFO level; developers can override the log level or attach
additional handlers as needed.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator, Optional


def get_logger(name: str = "sf_streamlit_utils") -> logging.Logger:
    """Return a module logger configured with a sensible default handler.

    The first time this function is called it attaches a ``StreamHandler``
    with a simple format to the logger.  Subsequent calls return the same
    logger without adding additional handlers.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@contextmanager
def timed_operation(operation: str, logger: Optional[logging.Logger] = None) -> Iterator[None]:
    """Context manager that logs the duration of an operation.

    Example::

        with timed_operation("query customers"):
            df = read_df(...)

    The start and end times (in seconds) will be logged at INFO level.
    """
    log = logger or get_logger()
    start_time = time.perf_counter()
    log.debug(f"Starting {operation}...")
    try:
        yield
    finally:
        end_time = time.perf_counter()
        duration = end_time - start_time
        log.info(f"{operation} completed in {duration:.3f}s")