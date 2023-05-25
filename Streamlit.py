#streamlit/secrets.toml


#[connections.snowpark]
account = "hs58126.central-india.azure"
user = "NishaantSoni"
password = "Htek@#1010"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "PETS"
schema = "PUBLIC"
client_session_keep_alive = True

# streamlit_app.py

# streamlit_app.py

import streamlit as st

# Initialize connection.
conn = st.experimental_connection('snowpark')

# Load the table as a dataframe using the Snowpark Session.
@st.cache_data
def load_table():
    with conn.safe_session() as session:
        return session.table('mytable').to_pandas()

df = load_table()

# Print results.
for row in df.itertuples():
    st.write(f"{row.NAME} has a :{row.PET}:")
