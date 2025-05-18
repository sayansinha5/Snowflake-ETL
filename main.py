import streamlit as st
import pandas as pd
from etl import run_etl

st.title("Snowflake ETL Demo")

uploaded_file = st.file_uploader("Upload users.csv", type="csv")

if uploaded_file:
    with open("users.csv", "wb") as f:
        f.write(uploaded_file.getvalue())

    st.success("File uploaded!")

    if st.button("Run ETL"):
        with st.spinner("Running ETL..."):
            df = run_etl("users.csv")
            st.success("ETL Completed!")
            st.dataframe(df)
