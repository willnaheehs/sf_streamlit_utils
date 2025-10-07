"""Configuration helpers for sf_streamlit_utils.

This module defines a dataclass for Snowflake connection settings and helper
functions to load configuration from environment variables or the Streamlit
`secrets.toml` file.  It centralises all of the keys used by the package so
developers don’t have to remember multiple names when configuring their
connections.  If `streamlit` isn’t installed or `st.secrets` is empty, only
environment variables will be used.

The keys in `st.secrets` should be grouped under a top‑level ``snowflake``
section, for example::

    [snowflake]
    account   = "orgname-account"
    user      = "APP_USER"
    password  = "*****"
    warehouse = "WH_DEV"
    role      = "ROLE_READONLY"
    database  = "MY_DB"
    schema    = "PUBLIC"

Any additional keys will be passed directly to the Snowflake connector.

Note that many of these values can also be provided via environment variables,
prefixed with ``SNOWFLAKE_`` (e.g. ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``).

"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import os


@dataclass
class SnowflakeConfig:
    """A container for Snowflake connection parameters.

    Attributes mirror the parameters accepted by the Snowflake Python connector.
    Fields that are ``None`` are omitted when constructing the connection
    dictionary.
    """

    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_key_file: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    token: Optional[str] = None
    authenticator: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    client_session_keepalive: Optional[bool] = True
    login_timeout: Optional[int] = None
    network_timeout: Optional[int] = None
    retries: Optional[int] = None
    retry_delay: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary of non‐None parameters suitable for the Snowflake connector.

        Extra parameters are merged into the result.  ``client_session_keepalive``
        defaults to ``True`` if not explicitly set.
        """
        params: Dict[str, Any] = {}
        for field_name, value in self.__dict__.items():
            if field_name == "extra":
                continue
            if value is not None:
                params[field_name] = value
        # Merge any extra parameters defined by the user
        params.update(self.extra)
        # Default to keep sessions alive unless explicitly disabled
        if "client_session_keepalive" not in params:
            params["client_session_keepalive"] = True
        return params


def load_from_env(prefix: str = "SNOWFLAKE_") -> SnowflakeConfig:
    """Load configuration values from environment variables.

    Environment variable names are expected to be prefixed with
    ``prefix`` (default ``SNOWFLAKE_``) and uppercase.  For example,
    ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``, etc.  Any environment
    variable starting with the prefix will be collected into the
    ``extra`` dictionary if it does not correspond to a known field.

    Returns a :class:`SnowflakeConfig` instance.
    """
    fields = {f.name for f in SnowflakeConfig.__dataclass_fields__.values() if f.name != "extra"}
    config_kwargs: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for env_name, env_value in os.environ.items():
        if not env_name.startswith(prefix):
            continue
        key = env_name[len(prefix) :].lower()
        # convert to expected field names (e.g. private_key_file)
        if key in fields:
            config_kwargs[key] = env_value
        else:
            extra[key] = env_value
    config = SnowflakeConfig(**config_kwargs)
    config.extra.update(extra)
    return config


def load_from_secrets() -> Optional[SnowflakeConfig]:
    """Load configuration from Streamlit's ``st.secrets``.

    If Streamlit is not installed or there is no ``snowflake`` section,
    this function returns ``None``.  If a ``snowflake`` section exists,
    its keys are mapped onto the :class:`SnowflakeConfig` fields.  Any
    unknown keys are placed into the ``extra`` dictionary.
    """
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return None
    secrets = getattr(st, "secrets", None)
    if not secrets:
        return None
    if "snowflake" not in secrets:
        return None
    sf_section = secrets["snowflake"]
    fields = {f.name for f in SnowflakeConfig.__dataclass_fields__.values() if f.name != "extra"}
    config_kwargs: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}
    for key, value in sf_section.items():
        lower_key = key.lower()
        if lower_key in fields:
            config_kwargs[lower_key] = value
        else:
            extra[lower_key] = value
    config = SnowflakeConfig(**config_kwargs)
    config.extra.update(extra)
    return config


def resolve_config(config: Optional[SnowflakeConfig] = None) -> SnowflakeConfig:
    """Resolve the effective Snowflake configuration.

    Priority is:

    1. Explicitly provided config object
    2. Streamlit secrets ``snowflake`` section
    3. Environment variables

    Values loaded from later sources will override earlier ones.  For example,
    environment variables override values from the secrets file.  This allows
    developers to override individual settings during local development.
    """
    # Start with secrets if available
    secrets_config = load_from_secrets()
    env_config = load_from_env()
    if config is None:
        base = SnowflakeConfig()
    else:
        base = config
    # Merge secrets
    if secrets_config is not None:
        for k, v in secrets_config.__dict__.items():
            if k == "extra":
                base.extra.update(v)
            elif v is not None:
                setattr(base, k, v)
    # Merge environment variables (highest precedence)
    for k, v in env_config.__dict__.items():
        if k == "extra":
            base.extra.update(v)
        elif v is not None:
            setattr(base, k, v)
    return base