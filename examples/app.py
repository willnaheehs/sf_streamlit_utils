"""Example Streamlit app using sf_streamlit_utils.

To run this app install the package with the ``dev`` extra and then
execute ``streamlit run examples/app.py`` from the repository root.
Configure your Snowflake credentials in ``.streamlit/secrets.toml``
or via environment variables before running the app.
"""

# examples/app.py
import time
import streamlit as st

st.title("sf-streamlit-utils – local smoke test")

from sf_streamlit_utils import connect
st.write("Trying to build a connection object:")
try:
    conn = connect()
    st.success(f"Connected (type: {type(conn).__name__})")
except Exception as e:
    st.error(f"connect() raised: {e}")

st.divider()

# If your package provides read_df, cache demo:
try:
    from sf_streamlit_utils import read_df
    st.subheader("Cached read_df demo")
    sql = st.text_input("SQL", "SELECT CURRENT_TIMESTAMP() AS now")
    run = st.button("Run query")
    if run:
        t0 = time.time()
        df = read_df(sql)  # should use your package's caching under the hood (or Streamlit's cache)
        st.write(df)
        st.caption(f"elapsed: {time.time()-t0:.3f}s")
        st.info("Run the same query again – if cached, it should be nearly instant.")
except Exception as e:
    st.warning(f"read_df not available or error importing: {e}")
