import types
import sys
import pytest

from sf_streamlit_utils.config import SnowflakeConfig
from sf_streamlit_utils.connection import SnowflakeConnectionManager


def test_create_connection_calls_connector(monkeypatch):
    # Mock snowflake.connector.connect
    called = {}

    def mock_connect(**kwargs):
        called["kwargs"] = kwargs
        # Return a minimal connection object with is_closed attribute and cursor
        class MockCursor:
            def execute(self, sql, params=None):
                return None
            def fetchall(self):
                return [(1,)]
            @property
            def description(self):
                return [("col", None, None, None, None, None, None)]

        class MockConnection:
            def __init__(self):
                self.closed = False
            def cursor(self):
                return MockCursor()
            def is_closed(self):
                return self.closed
        return MockConnection()

    snowflake_module = types.SimpleNamespace(connector=types.SimpleNamespace(connect=mock_connect))
    monkeypatch.setitem(sys.modules, "snowflake.connector", snowflake_module.connector)
    cfg = SnowflakeConfig(account="acct", user="user", password="pass")
    manager = SnowflakeConnectionManager(cfg)
    conn = manager.get_connection()
    # Ensure connect was called with the account, user and password
    assert called["kwargs"]["account"] == "acct"
    assert called["kwargs"]["user"] == "user"
    assert called["kwargs"]["password"] == "pass"
    # Connection should be reused on subsequent calls
    same_conn = manager.get_connection()
    assert conn is same_conn


def test_is_closed_reconnects(monkeypatch):
    # Simulate a connection that is closed on first call
    class MockCursor:
        def execute(self, sql, params=None):
            return None
        def fetchall(self):
            return [(1,)]
        @property
        def description(self):
            return [("col", None, None, None, None, None, None)]

    class MockConnection:
        def __init__(self, closed):
            self.closed = closed
        def cursor(self):
            return MockCursor()
        def is_closed(self):
            return self.closed

    connections = []

    def mock_connect(**kwargs):
        # Alternate between closed and open connections
        if not connections:
            conn = MockConnection(closed=True)
        else:
            conn = MockConnection(closed=False)
        connections.append(conn)
        return conn

    snowflake_module = types.SimpleNamespace(connector=types.SimpleNamespace(connect=mock_connect))
    monkeypatch.setitem(sys.modules, "snowflake.connector", snowflake_module.connector)
    cfg = SnowflakeConfig(account="acct", user="user", password="pass")
    manager = SnowflakeConnectionManager(cfg)
    # First call should detect closed and reconnect
    conn = manager.get_connection()
    assert len(connections) == 2  # Should have created two connections
    assert not conn.is_closed()