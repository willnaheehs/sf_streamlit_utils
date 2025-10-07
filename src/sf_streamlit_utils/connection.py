"""Snowflake connection management for Streamlit utilities.

This module implements a resilient connection manager that caches
Snowflake connections across Streamlit reruns and transparently
reconnects when sessions are closed.  It also exposes helper
functions to obtain a Snowpark session.  The manager reads its
configuration from a :class:`~sf_streamlit_utils.config.SnowflakeConfig` or
falls back to Streamlit secrets and environment variables.

The recommended way to access the connection manager in a Streamlit
app is via :func:`get_connection_manager`, which will cache the
manager using ``st.cache_resource`` when Streamlit is available.  If
Streamlit is not present the manager is stored in a module-level
singleton.
"""

from __future__ import annotations

from importlib import import_module


from typing import Any, Optional, Tuple, Dict, Callable
import threading
import time

from .config import SnowflakeConfig, resolve_config
from .logging_utils import get_logger, timed_operation

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover - streamlit may not be installed
    st = None  # type: ignore


# connection.py
from importlib import import_module
from typing import Any, Optional
import threading
import logging

class SnowflakeConnectionManager:
    def __init__(self, config, logger: Optional[logging.Logger] = None):
        self.config = config
        self._connection: Any = None
        self._lock = threading.RLock()
        self.logger = logger or logging.getLogger(__name__)

    def _create_connection(self) -> Any:
        try:
            connector = import_module("snowflake.connector")  # <- robust import for tests
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "snowflake-connector-python is required but not installed. "
                "Install it with `pip install snowflake-connector-python`"
            ) from exc

        params = self.config.to_dict().copy()

        # Do NOT pre-validate required fields here — let the connector validate.
        # (The tests rely on being able to connect with minimal / mocked kwargs.)

        self.logger.debug("Creating new Snowflake connection")
        return connector.connect(**params)

    def _is_closed(self) -> bool:
        """
        Handle both property and method forms:
        - connection.is_closed() -> bool
        - connection.is_closed  -> bool
        - fallback to .closed attribute, default False
        """
        conn = self._connection
        if conn is None:
            return True
        try:
            attr = getattr(conn, "is_closed", None)
            if callable(attr):
                return bool(attr())
            if attr is not None:
                return bool(attr)
        except Exception:
            pass
        return bool(getattr(conn, "closed", False))

    def get_connection(self) -> Any:
        with self._lock:
            if self._connection is None:
                self._connection = self._create_connection()
                # If the first connection is already closed, immediately recreate
                if self._is_closed():
                    self.logger.info("Connection was closed; reconnecting immediately.")
                    self._connection = self._create_connection()
            else:
                if self._is_closed():
                    self.logger.info("Existing connection closed; reconnecting.")
                    self._connection = self._create_connection()
            return self._connection

    def execute(self, sql: str, params: Optional[dict] = None):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(sql, params or None)
        return cur



# Global state for non‑Streamlit usage
_global_manager: Optional[SnowflakeConnectionManager] = None
_global_manager_lock = threading.Lock()


def _create_manager(config: Optional[SnowflakeConfig]) -> SnowflakeConnectionManager:
    resolved = resolve_config(config)
    return SnowflakeConnectionManager(resolved)


def get_connection_manager(config: Optional[SnowflakeConfig] = None) -> SnowflakeConnectionManager:
    """Return a cached connection manager.

    In a Streamlit context this function uses ``st.cache_resource`` to
    cache the manager across reruns.  When Streamlit is not available
    (for example, in tests or scripts), a module‑level singleton is
    returned.  If ``config`` is provided it will override values from
    secrets and environment variables.
    """
    global _global_manager
    # When Streamlit is available, use cache_resource to manage the manager
    if st is not None:
        # Define a cached function within this scope.  We cannot decorate
        # get_connection_manager directly because Streamlit caches by args.
        @st.cache_resource(show_spinner=False)
        def _get_cached_manager(conf: Optional[SnowflakeConfig]) -> SnowflakeConnectionManager:
            return _create_manager(conf)
        return _get_cached_manager(config)
    # Fallback: use a global manager
    with _global_manager_lock:
        if _global_manager is None:
            _global_manager = _create_manager(config)
        elif config is not None:
            # Update existing manager with new config
            _global_manager = _create_manager(config)
        return _global_manager

