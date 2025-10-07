# sf-streamlit-utils

`sf-streamlit-utils` is a lightweight, Streamlit‑focused Python package that wraps Snowflake’s Python connector and Snowpark API to give your team a consistent, secure and ergonomic way to query and display Snowflake data across multiple Streamlit apps. The package reduces boilerplate, enforces safe defaults and adds observability so developers can focus on building great data applications.

## Why this package?

Building database‑driven Streamlit apps often involves copying the same connection setup, caching decorators and query helpers from one project to the next. Streamlit re‑runs your script from top to bottom on every user interaction, so without caching you end up reconnecting and re‑querying Snowflake unnecessarily. Streamlit provides a built‑in caching system where functions decorated with `st.cache_data` will run once and reuse the result across reruns【328993432117158†L140-L170】. Likewise, the Streamlit connection API caches queries indefinitely unless a `ttl` is provided【378922438068066†L470-L486】. This package standardises those patterns and wraps them in easy‑to‑use helper functions.

Snowflake’s own guidance for using Streamlit recommends encapsulating connection logic in a class, caching the object and re‑creating it if the underlying connection has been closed【173727619445549†L140-L178】. `sf-streamlit-utils` follows this pattern to provide a singleton connection manager that reads credentials from `st.secrets` or environment variables, connects with the Snowflake connector or Snowpark, and automatically reconnects when necessary. It also exposes a simple API for querying data with parameterised SQL while leveraging Streamlit’s caching to avoid redundant work.

## Features

* **Standardised connection management** – configure credentials via `st.secrets`, environment variables or Python objects and let the package handle key‑pair or password authentication. A singleton connection manager caches the connection across reruns and transparently reconnects when idle connections are closed【173727619445549†L140-L178】.
* **Resilient caching** – query functions decorated with `st.cache_data` store the result based on a hash of the SQL text and parameters. Results are returned instantly on subsequent calls with the same query and parameters, avoiding re‑running long queries【328993432117158†L140-L170】.
* **Helper functions** – `connect()` returns an authenticated connection object; `read_df(sql, params)` runs parameterised SQL and returns a pandas `DataFrame`; `write_df(df, table)` writes a `DataFrame` to a Snowflake table; and `stage_dataframe()` stages large data sets to Snowflake’s internal stage before loading.
* **Safe defaults** – enforce default roles and warehouses to prevent unexpected privilege escalation or excessive compute usage. Developers can specify read‑only roles or small warehouses for development and the package will apply them automatically.
* **Schema browser (optional)** – a Streamlit component that introspects the current database and displays available databases, schemas, tables and columns. Users can select tables/columns to generate SQL snippets.
* **Observability** – built‑in logging measures execution time for each query, reports cache hits/misses and surfaces structured errors when queries fail.
* **Example app and tests** – includes a sample Streamlit app demonstrating typical usage and unit tests using `pytest` and `pytest‑mock`.
* **Continuous integration (CI)** – a GitHub Actions workflow builds and publishes the package to PyPI and can deploy a template app.

## Installation

Install from PyPI:

```bash
pip install sf-streamlit-utils
```

Alternatively, install the development version from source:

```bash
git clone https://github.com/your-org/sf-streamlit-utils.git
cd sf-streamlit-utils
pip install -e .[dev]
```

You will need Python 3.8+ and the Snowflake connector packages. The `pyproject.toml` lists these dependencies. The optional `dev` extra installs tools for testing and code formatting.

## Quick start

1. **Configure your credentials.** The package looks for a `snowflake` section in `st.secrets` (recommended for Streamlit Community Cloud). At minimum provide the account locator, username and authentication details. For example:

```toml
[snowflake]
account = "orgname-accountname"
user    = "APP_USER"
password = "*****"  # or private_key_file for key‑pair auth
warehouse = "WH_DEV"
role      = "ROLE_READONLY"
database  = "MY_DB"
schema    = "PUBLIC"
```

If you are not using Streamlit secrets you can set equivalent environment variables (e.g. `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.).

2. **Use the helper API in your Streamlit app.** For example:

```python
from sf_streamlit_utils import connect, read_df, write_df

# Establish the connection (cached)
conn = connect()

# Run a parameterised query and get a DataFrame
query = "SELECT * FROM my_table WHERE id > %s"
df = read_df(query, params=(1000,))

# Display the DataFrame using Streamlit
import streamlit as st
st.write(df)

# Write a DataFrame back to Snowflake
write_df(df, table="MY_SCHEMA.MY_TABLE")
```

`connect()` returns a managed connection object that can also be used directly with the Snowflake connector or Snowpark APIs. Under the hood, the connection is created once and reused on subsequent calls, and it automatically reconnects if Snowflake closes the session due to inactivity【173727619445549†L140-L178】.

## Authentication options

By default, the package uses username/password authentication with the Snowflake Python connector. For production deployments you should use key‑pair authentication or OAuth. To enable key‑pair authentication, provide `private_key` or `private_key_file` and `private_key_passphrase` in your secret configuration. For OAuth, set `auth_type = "oauth"` and provide `token` and `authenticator` (e.g. `externalbrowser`). See the Snowflake connector documentation for details.

## Safe defaults and governance

Snowflake accounts often have multiple roles and warehouses. To avoid “foot‑guns,” the connection manager enforces sensible defaults:

* The configured `warehouse` and `role` are used on every connection. You can override them per‑query by passing different values to `connect()` or `read_df()` but the defaults prevent accidentally using privileged roles.
* Query concurrency is limited (by default to one concurrent query) and the connection manager implements retries with exponential back‑off when Snowflake returns transient errors. You can configure the retry count and delay through environment variables.
* Large result sets can exceed Streamlit’s message size limits in Snowflake. We encourage you to filter and aggregate data in Snowflake before returning results【339831608578344†L190-L196】.

## Development and testing

Clone the repository and install the development dependencies (`pip install -e .[dev]`). Run the test suite with:

```bash
pytest
```

The tests use mocks to avoid hitting a real Snowflake database. If you wish to run integration tests against a real account, set the appropriate environment variables and use the `--run-integration` flag.

### Example app

See `examples/app.py` for a minimal Streamlit application that uses the helper functions to browse tables and run queries. Launch it with:

```bash
streamlit run examples/app.py
```

## License

This project is licensed under the MIT License. See `LICENSE` for the full text.