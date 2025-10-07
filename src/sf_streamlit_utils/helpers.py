"""Public API helpers for sf_streamlit_utils.

This module exposes a simplified set of functions for app developers.  The
helpers wrap the lower‑level connection and caching functionality into a
concise API.  For most use cases you can import functions directly
from this module instead of digging into the submodules.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Tuple, Dict, Union
import logging

try:
    import pandas as pd  # type: ignore
except ImportError:  # pragma: no cover
    pd = None  # type: ignore

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None  # type: ignore

from .config import SnowflakeConfig, resolve_config
from .connection import get_connection_manager
from .cache import cached_read_df, _normalise_params
from .logging_utils import timed_operation, get_logger

__all__ = [
    "connect",
    "read_df",
    "write_df",
    "stage_dataframe",
    "browse_schema",
    "get_connection_manager",
]


def connect(config: Optional[Union[SnowflakeConfig, Dict[str, Any]]] = None) -> Any:
    """Return a live Snowflake connection.

    If ``config`` is a dictionary it will be converted to a
    :class:`SnowflakeConfig`.  Otherwise the configuration is resolved
    from Streamlit secrets and environment variables.
    """
    cfg: Optional[SnowflakeConfig]
    if config is None:
        cfg = None
    elif isinstance(config, SnowflakeConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SnowflakeConfig(**config)  # type: ignore[arg-type]
    else:
        raise TypeError("config must be a SnowflakeConfig or dict")
    manager = get_connection_manager(cfg)
    return manager.get_connection()


def read_df(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    *,
    ttl: Optional[float] = None,
    show_spinner: bool | str = True,
    config: Optional[Union[SnowflakeConfig, Dict[str, Any]]] = None,
    raw_cursor: bool = False,
) -> Union["pd.DataFrame", Any]:
    """Execute a SQL query and return its results.

    This function chooses between cached and uncached execution based
    on whether Streamlit is available.  When Streamlit is present
    (i.e. you are running inside a Streamlit app), the query is
    cached using :func:`st.cache_data`.  Outside of Streamlit the
    query is executed without caching.  Passing ``raw_cursor=True``
    returns the underlying cursor instead of a DataFrame.

    Parameters
    ----------
    sql:
        The SQL text to execute.
    params:
        Optional bind parameters.  See :func:`cached_read_df` for details.
    ttl:
        Optional cache time‑to‑live in seconds.  Ignored when not running
        under Streamlit.
    show_spinner:
        Whether to show Streamlit’s spinner while executing the query.  Only
        relevant when Streamlit is available.
    config:
        Optional configuration override to create a new connection manager.
    raw_cursor:
        If ``True``, return the raw cursor instead of a DataFrame.  This can
        be useful if you plan to call ``fetchall()`` or ``fetchone()``
        yourself.  Note that caching will still occur if Streamlit is
        available.
    """
    # If running under Streamlit use cached query
    if st is not None and not raw_cursor:
        return cached_read_df(sql, params=params, ttl=ttl, show_spinner=show_spinner)
    # Otherwise run without caching
    cfg: Optional[SnowflakeConfig]
    if config is None:
        cfg = None
    elif isinstance(config, SnowflakeConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SnowflakeConfig(**config)  # type: ignore[arg-type]
    else:
        raise TypeError("config must be a SnowflakeConfig or dict")
    manager = get_connection_manager(cfg)
    bind_params = _normalise_params(params)
    with timed_operation(f"Query: {sql}", logger=get_logger(__name__)):
        cur = manager.execute(sql, bind_params)
        if raw_cursor:
            return cur
        try:
            df = cur.fetch_pandas_all()
        except Exception:
            rows = cur.fetchall()
            if pd is None:
                raise RuntimeError(
                    "pandas is required to build DataFrame. Install pandas or use raw_cursor=True."
                )
            df = pd.DataFrame.from_records(rows, columns=[c[0] for c in cur.description])
        return df


def write_df(
    df: "pd.DataFrame",
    table: str,
    *,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    chunk_size: int = 16000,
    auto_create_table: bool = True,
    overwrite: bool = False,
    config: Optional[Union[SnowflakeConfig, Dict[str, Any]]] = None,
) -> Tuple[bool, int, int, list]:
    """Write a pandas DataFrame into a Snowflake table.

    This helper uses Snowflake’s ``write_pandas`` utility to load a DataFrame
    directly into a table.  The DataFrame’s column names must match
    the destination table or ``auto_create_table`` must be set to
    ``True`` so that ``write_pandas`` creates the table automatically.

    Parameters
    ----------
    df:
        The pandas DataFrame to write.
    table:
        Name of the destination table (optionally qualified with schema
        and database).
    database:
        Optional database name.  Overrides the value from the config.
    schema:
        Optional schema name.  Overrides the value from the config.
    chunk_size:
        Number of rows per chunk to upload.  Larger chunks reduce the
        number of files created in the stage, but each chunk must fit
        within the memory limits of the environment.
    auto_create_table:
        If ``True``, ``write_pandas`` will attempt to create the table if
        it does not exist.
    overwrite:
        If ``True``, truncate the table before writing.  Otherwise,
        append rows.
    config:
        Optional configuration override.

    Returns
    -------
    tuple
        ``(success, nchunks, nrows, output)`` as returned by
        ``snowflake.connector.pandas_tools.write_pandas``.
    """
    if df is None:
        raise ValueError("df must not be None")
    if pd is None:
        raise RuntimeError("pandas is required for write_df. Install pandas.")
    cfg: Optional[SnowflakeConfig]
    if config is None:
        cfg = None
    elif isinstance(config, SnowflakeConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SnowflakeConfig(**config)  # type: ignore[arg-type]
    else:
        raise TypeError("config must be a SnowflakeConfig or dict")
    manager = get_connection_manager(cfg)
    conn = manager.get_connection()
    # Attempt to import write_pandas lazily
    try:
        from snowflake.connector.pandas_tools import write_pandas  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "snowflake-connector-python is required for write_df. Install snowflake-connector-python"
        ) from exc
    # Determine database and schema
    db = database or manager.config.database
    sch = schema or manager.config.schema
    # Determine if we should overwrite existing rows
    # Write_pandas does not support overwrite directly; if overwrite is True we truncate first
    if overwrite:
        with timed_operation(f"Truncate table {table}", logger=get_logger(__name__)):
            truncate_sql = f"TRUNCATE TABLE {table}"
            manager.execute(truncate_sql)
    with timed_operation(f"Write DataFrame to {table}", logger=get_logger(__name__)):
        return write_pandas(
            conn,
            df=df,
            table_name=table,
            database=db,
            schema=sch,
            chunk_size=chunk_size,
            auto_create_table=auto_create_table,
        )


def stage_dataframe(
    df: "pd.DataFrame",
    stage: str,
    *,
    file_format: str = "csv",
    compression: str = "gzip",
    config: Optional[Union[SnowflakeConfig, Dict[str, Any]]] = None,
) -> str:
    """Stage a DataFrame as a file in a Snowflake stage.

    This helper serialises the DataFrame to a temporary file, uploads it
    into the specified stage via the Snowflake ``PUT`` command and
    returns the path of the staged file.  Note that this is a simple
    helper; for large data sets or production use consider using
    Snowpark’s ``write_pandas`` or Snowflake’s external stages.

    Parameters
    ----------
    df:
        The pandas DataFrame to stage.
    stage:
        The fully qualified name of the Snowflake stage (e.g. ``@my_stage``).
    file_format:
        File format to use when serialising the DataFrame.  Only ``csv``
        is currently supported.
    compression:
        Compression type (e.g. ``gzip``, ``none``).
    config:
        Optional configuration override.

    Returns
    -------
    str
        The path of the file in the stage.
    """
    if pd is None:
        raise RuntimeError("pandas is required for staging. Install pandas.")
    cfg: Optional[SnowflakeConfig]
    if config is None:
        cfg = None
    elif isinstance(config, SnowflakeConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SnowflakeConfig(**config)  # type: ignore[arg-type]
    else:
        raise TypeError("config must be a SnowflakeConfig or dict")
    manager = get_connection_manager(cfg)
    conn = manager.get_connection()
    log = get_logger(__name__)
    # Serialise DataFrame to a temporary file
    import tempfile
    import os
    import csv
    import io
    # Use pandas to create CSV in memory
    with timed_operation("Serialise DataFrame for staging", logger=log):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
    # Determine a unique file name
    import uuid
    file_name = f"{uuid.uuid4().hex}.csv"
    remote_path = f"{stage}/{file_name}"
    # Upload via PUT
    with timed_operation(f"Upload {file_name} to stage {stage}", logger=log):
        cur = conn.cursor()
        put_sql = f"PUT file://- {remote_path} OVERWRITE = TRUE AUTO_COMPRESS = {str(compression != 'none').upper()}"
        # The Snowflake connector supports uploading file streams via the PUT command by passing the file via the cursor
        try:
            cur.upload_stream(csv_buffer, remote_path, compress=(compression != "none"))
        except AttributeError:
            # upload_stream may not be available in older connector versions; fall back to PUT
            # Write the buffer to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(csv_buffer.getvalue().encode("utf-8"))
                tmp.flush()
                tmp_path = tmp.name
            # Use PUT command to upload
            cur.execute(f"PUT file://{tmp_path} {remote_path} OVERWRITE = TRUE")
            os.unlink(tmp_path)
    return remote_path


def browse_schema(*, config: Optional[Union[SnowflakeConfig, Dict[str, Any]]] = None) -> None:
    """Render a simple schema browser in a Streamlit app.

    This helper introspects the current Snowflake account and allows
    users to browse available databases, schemas, tables and columns via
    select boxes.  It generates SQL snippets based on the selected
    table and columns and inserts them into a text area for easy copy
    and paste.  Requires Streamlit; outside of Streamlit this function
    does nothing.
    """
    if st is None:
        raise RuntimeError("browse_schema can only be used inside a Streamlit app.")
    cfg: Optional[SnowflakeConfig]
    if config is None:
        cfg = None
    elif isinstance(config, SnowflakeConfig):
        cfg = config
    elif isinstance(config, dict):
        cfg = SnowflakeConfig(**config)  # type: ignore[arg-type]
    else:
        raise TypeError("config must be a SnowflakeConfig or dict")
    manager = get_connection_manager(cfg)
    conn = manager.get_connection()
    cur = conn.cursor()
    # Fetch list of databases
    cur.execute("SHOW DATABASES")
    dbs = [row[1] for row in cur.fetchall()]  # database_name is the second column in SHOW DATABASES output
    selected_db = st.sidebar.selectbox("Database", dbs)
    # Set the current database to reduce latency
    manager.execute(f"USE DATABASE {selected_db}")
    # Fetch schemas
    cur.execute("SHOW SCHEMAS")
    schemas = [row[1] for row in cur.fetchall()]
    selected_schema = st.sidebar.selectbox("Schema", schemas)
    manager.execute(f"USE SCHEMA {selected_schema}")
    # Fetch tables
    cur.execute("SHOW TABLES")
    tables = [row[1] for row in cur.fetchall()]
    selected_table = st.sidebar.selectbox("Table", tables)
    # Fetch columns for selected table
    cur.execute(f"DESCRIBE TABLE {selected_table}")
    columns = [row[0] for row in cur.fetchall()]
    selected_cols = st.sidebar.multiselect("Columns", columns, default=columns)
    # Generate SQL snippet
    col_list = ", ".join(selected_cols)
    sql_snippet = f"SELECT {col_list} FROM {selected_db}.{selected_schema}.{selected_table}"
    st.text_area("SQL", value=sql_snippet, height=80)