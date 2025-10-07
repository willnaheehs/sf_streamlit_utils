import types
import sys
import pytest

from sf_streamlit_utils.config import SnowflakeConfig
from sf_streamlit_utils.helpers import read_df, write_df, connect

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None  # pragma: no cover


def test_read_df_without_streamlit(monkeypatch):
    # Remove streamlit from sys.modules to simulate non‑Streamlit context
    monkeypatch.setitem(sys.modules, "streamlit", None)

    # Mock snowflake connector
    class MockCursor:
        def __init__(self):
            self.description = [("id", None, None, None, None, None, None)]
        def execute(self, sql, params=None):
            pass
        def fetch_pandas_all(self):
            if pd is None:
                raise AttributeError
            return pd.DataFrame({"id": [1, 2, 3]})
        def fetchall(self):
            return [(1,), (2,), (3,)]

    class MockConnection:
        def cursor(self):
            return MockCursor()
        def is_closed(self):
            return False

    def mock_connect(**kwargs):
        return MockConnection()

    snowflake_module = types.SimpleNamespace(connector=types.SimpleNamespace(connect=mock_connect))
    monkeypatch.setitem(sys.modules, "snowflake.connector", snowflake_module.connector)
    # Execute read_df and verify DataFrame
    df = read_df("SELECT * FROM test", params=None)
    if pd:
        assert list(df["id"]) == [1, 2, 3]
    else:
        # When pandas isn't available raw execution should still work
        assert df == [(1,), (2,), (3,)]  # type: ignore[comparison-overlap]


def test_write_df(monkeypatch):
    if pd is None:
        pytest.skip("pandas is required for write_df test")
    # Remove streamlit from sys.modules to avoid caching
    monkeypatch.setitem(sys.modules, "streamlit", None)
    # Mock write_pandas
    captured = {}
    def mock_write_pandas(conn, df, table_name, database=None, schema=None, chunk_size=None, auto_create_table=True):
        captured["df"] = df
        captured["table_name"] = table_name
        return True, 1, len(df), []
    # Mock snowflake connection
    class MockConnection:
        def __init__(self):
            self.closed = False
        def cursor(self):
            class Dummy:
                def execute(self, sql, params=None):
                    pass
            return Dummy()
        def is_closed(self):
            return self.closed
    def mock_connect(**kwargs):
        return MockConnection()
    snowflake_module = types.SimpleNamespace(
        connector=types.SimpleNamespace(connect=mock_connect, pandas_tools=types.SimpleNamespace(write_pandas=mock_write_pandas))
    )
    monkeypatch.setitem(sys.modules, "snowflake.connector", snowflake_module.connector)
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", types.SimpleNamespace(write_pandas=mock_write_pandas))
    # Create DataFrame
    df = pd.DataFrame({"id": [1, 2]})
    success, nchunks, nrows, output = write_df(df, table="TEST_TABLE", database="DB", schema="SCH")
    assert success is True
    assert nrows == 2
    assert captured["table_name"] == "TEST_TABLE"