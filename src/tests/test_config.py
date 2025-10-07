import os
import types
import sys
import pytest

from sf_streamlit_utils.config import SnowflakeConfig, load_from_env, load_from_secrets, resolve_config


def test_load_from_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pass")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "role")
    cfg = load_from_env()
    assert cfg.account == "acct"
    assert cfg.user == "user"
    assert cfg.password == "pass"
    assert cfg.role == "role"


def test_load_from_secrets(monkeypatch):
    # Simulate streamlit.secrets
    secrets_module = types.SimpleNamespace()
    secrets_module.secrets = {
        "snowflake": {
            "account": "acct",
            "user": "user",
            "password": "pass",
            "warehouse": "wh",
        }
    }
    monkeypatch.setitem(sys.modules, "streamlit", secrets_module)
    cfg = load_from_secrets()
    assert cfg.account == "acct"
    assert cfg.user == "user"
    assert cfg.password == "pass"
    assert cfg.warehouse == "wh"


def test_resolve_config_env_overrides_secrets(monkeypatch):
    # Simulate secrets
    secrets_module = types.SimpleNamespace()
    secrets_module.secrets = {
        "snowflake": {
            "account": "acct_secr",
            "user": "user_secr",
        }
    }
    monkeypatch.setitem(sys.modules, "streamlit", secrets_module)
    # Set env to override user
    monkeypatch.setenv("SNOWFLAKE_USER", "user_env")
    cfg = resolve_config()
    # account comes from secrets; user from env
    assert cfg.account == "acct_secr"
    assert cfg.user == "user_env"