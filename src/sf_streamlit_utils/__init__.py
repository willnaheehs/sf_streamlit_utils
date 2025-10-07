"""sf-streamlit-utils

This package provides utilities to simplify building Streamlit apps that
interact with Snowflake.  It exposes functions for connection
management, query caching, data loading and staging, plus an optional
component for browsing schema metadata.  See :mod:`sf_streamlit_utils.helpers`
for the main API.

The top‑level namespace re‑exports the most commonly used classes and
functions:

* :func:`connect` – obtain a managed Snowflake connection
* :func:`read_df` – execute a SQL query and return a DataFrame
* :func:`write_df` – write a DataFrame into a Snowflake table
* :func:`stage_dataframe` – stage a DataFrame as a file in a stage
* :func:`browse_schema` – interactive schema browser for Streamlit apps
* :class:`SnowflakeConfig` – configuration dataclass
* :func:`resolve_config` – merge secrets, environment variables and user overrides
* :func:`get_connection_manager` – obtain the underlying connection manager
"""

from .helpers import connect, read_df, write_df, stage_dataframe, browse_schema, get_connection_manager
from .config import SnowflakeConfig, resolve_config, load_from_env, load_from_secrets

__all__ = [
    "connect",
    "read_df",
    "write_df",
    "stage_dataframe",
    "browse_schema",
    "SnowflakeConfig",
    "resolve_config",
    "load_from_env",
    "load_from_secrets",
    "get_connection_manager",
]