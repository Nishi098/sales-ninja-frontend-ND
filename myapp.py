import streamlit as st
import pandas as pd
import plotly.express as px
from dateutil.parser import parse

# Load data from same folder and sample 10%
df_full_ = pd.read_csv("data_dashboard_merged_v2.csv", nrows = 10000)  # Replace with your actual filename
df = df_full_.sample(frac=0.1, random_state=42)  # 10% random sample



from dateutil.parser import parse

min_date = parse("2007-01-01").strftime('%Y-%m-%d')  # e.g. '2007-01-01'
max_date = parse("2009-12-31").strftime('%Y-%m-%d')  # e.g. '2009-12-31'

query = f"""
    select * from `dashboard_data.dashboard_merged_data_5`
    WHERE DateKey BETWEEN '{min_date}' AND '{max_date}'
    ORDER BY DateKey
"""

df_full = pd.read_gbq(query,project_id='nodal-clock-456815-g3')  # Assuming you have a BigQuery client set up
df = df_full.sample(frac=0.1, random_state=42)  # 10% random sample



# Title
st.title("My Dashboard")

# Show data
st.subheader("Data Overview")
st.write(f"Original rows: {len(df_full)}")
st.write(f"Sampled rows: {len(df)} (10%)")
st.dataframe(df)

# Basic chart - adjust column names to match your CSV
st.subheader("Charts")

# Bar chart (replace 'column1' with your actual column name)
if 'SalesAmount' in df.columns:
    fig = px.bar(df, x='SalesAmount', title="Bar Chart")
    st.plotly_chart(fig)

# Line chart (replace 'column2' with your actual column name)
if 'SalesAmount' in df.columns:
    fig = px.line(df, y='SalesAmount', title="Line Chart")
    st.plotly_chart(fig)
