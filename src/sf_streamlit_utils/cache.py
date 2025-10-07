"""Query caching helpers.

This module provides functions that wrap Snowflake queries in Streamlit’s
caching primitives.  Caching avoids re‑executing the same SQL across
multiple reruns and improves the performance of your apps.  Under
the hood it uses :func:`st.cache_data` to store the results of a
query based on the SQL text and its bind parameters.  Each unique
combination of SQL and parameters results in a separate cache entry.
"""
# sf_streamlit_utils/cache.py
from __future__ import annotations

from typing import Optional, Tuple, Any
from importlib import import_module
import sys, json

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None

from .connection import get_connection_manager


def _normalise_params(params: Optional[dict]) -> Optional[dict]:
    """Legacy export kept for compatibility. New cache logic ignores this."""
    return params if params else None

def _maybe_import_streamlit():
    # If tests set sys.modules["streamlit"] = None, treat as absent and don't import.
    mod = sys.modules.get("streamlit", ...)
    if mod is None:
        return None
    try:
        return import_module("streamlit")
    except Exception:
        return None


def _hashable_params(params: Optional[dict]) -> Tuple[Tuple[str, str], ...]:
    if not params:
        return ()
    # stable, hashable representation
    return tuple(sorted((k, json.dumps(v, sort_keys=True, default=str)) for k, v in params.items()))


def cached_read_df(sql: str, params: Optional[dict] = None, *, ttl: int = 0, show_spinner: bool = True):
    """
    Returns a DataFrame (or rows if pandas is unavailable).
    Uses st.cache_data when Streamlit is present; otherwise runs directly.
    """
    st = _maybe_import_streamlit()
    manager = get_connection_manager(None)
    hashed = _hashable_params(params)

    def _run(sql_text: str, hashed_params: Tuple[Tuple[str, str], ...]):
        bind = {k: json.loads(v) for k, v in hashed_params}
        cur = manager.execute(sql_text, bind or None)

        if pd is not None:
            try:
                return cur.fetch_pandas_all()
            except Exception:
                pass  # fall through to rows → DataFrame

        rows = cur.fetchall()
        if pd is None:
            return rows
        cols = [c[0] for c in getattr(cur, "description", [])] or None
        return pd.DataFrame.from_records(rows, columns=cols)

    if st is not None and hasattr(st, "cache_data"):
        @st.cache_data(ttl=ttl, show_spinner=show_spinner)
        def _impl(sql_text: str, hashed_params: Tuple[Tuple[str, str], ...]):
            return _run(sql_text, hashed_params)
    else:
        def _impl(sql_text: str, hashed_params: Tuple[Tuple[str, str], ...]):
            return _run(sql_text, hashed_params)

    return _impl(sql, hashed)



# def cached_read_df(
#     sql: str,
#     params: Optional[Iterable[Any]] = None,
#     *,
#     ttl: Optional[float] = None,
#     show_spinner: bool | str = True,
# ) -> "pd.DataFrame":
#     """Run a SQL query and return a pandas DataFrame, caching the result.

#     Parameters
#     ----------
#     sql:
#         The SQL query to execute.  Parameter placeholders should use
#         ``%s`` markers for positional parameters.
#     params:
#         Optional sequence or mapping of bind parameters.  Dictionaries
#         are converted into a JSON string for hashing purposes.
#     ttl:
#         Optional time‑to‑live for the cache entry in seconds.  If ``None``
#         (default) the result is cached indefinitely【378922438068066†L470-L486】.
#     show_spinner:
#         Whether to show Streamlit’s spinner while the query executes.  If
#         a string is provided it will be used as the spinner message.

#     Returns
#     -------
#     pandas.DataFrame
#         A DataFrame containing the query results.
#     """
#     if st is None:
#         raise RuntimeError(
#             "cached_read_df requires streamlit. Install streamlit or call read_df() instead."
#         )
#     manager = get_connection_manager()
#     # Normalise parameters for hashing
#     hashable_params = _normalise_params(params)
#     log = get_logger(__name__)

#     @st.cache_data(ttl=ttl, show_spinner=show_spinner)
#     def _run_query(sql_text: str, bind_params: Optional[Tuple[Any, ...]]) -> "pd.DataFrame":
#         with timed_operation(f"Query: {sql_text}", logger=log):
#             cur = manager.execute(sql_text, bind_params)
#             try:
#                 # Use Snowflake's pandas fetch if available for performance
#                 df = cur.fetch_pandas_all()
#             except Exception:
#                 # Fallback: fetch all rows and build DataFrame
#                 rows = cur.fetchall()
#                 if pd is None:
#                     raise RuntimeError(
#                         "pandas is required to build DataFrame. Install pandas or use a raw cursor."
#                     )
#                 df = pd.DataFrame.from_records(rows, columns=[c[0] for c in cur.description])
#             return df

#     return _run_query(sql, hashable_params)