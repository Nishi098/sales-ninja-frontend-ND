import streamlit as st
import pandas as pd
from dateutil.parser import parse
import requests
import datetime
import matplotlib.pyplot as plt
import datetime

#I need a dataset with dates for each day
#I need kumar's table to have data for each date
#I need my 0.1% sample to have data from each date
#I probably need my data to be multiplied by 1000

#1f. Load dull data for actuals
min_date_load = parse("2007-01-01").strftime('%Y-%m-%d')  # e.g. '2007-01-01'
max_date_load = parse("2009-12-31").strftime('%Y-%m-%d')  # e.g. '2009-12-31'

query = f"""
    select * from `dashboard_data.dashboard_merged_data_1`
    WHERE DateKey BETWEEN '{min_date_load}' AND '{max_date_load}'
    ORDER BY DateKey
"""


df_full = pd.read_gbq(query,project_id='nodal-clock-456815-g3')  # Assuming you have a BigQuery client set up
df = df_full.sample(frac=0.1, random_state=42)  # 10% random sample




#2f. Divide space into 4 columns: 1, 1, 1, 2 ratio
col1, col2, col3, col4 = st.columns([1, 1, 1, 2])


# 1v. Title of Page 1

st.markdown("""
<div style="
position:fixed;
top:1.4cm;
left:0;
width:100vw;
margin-left:calc(-50vw + 50%);
height:3cm;
background:linear-gradient(to right,white,#d8b4ff,#7f00ff);
display:flex;
align-items:center;
justify-content:center;
font-size:4rem;
font-weight:bold;
color:#4b0082;
z-index:9999;">
SALES DASHBOARD
</div>
""", unsafe_allow_html=True)


#2v. Sidebar

# Sidebar with a title
with st.sidebar:
    st.title("Filters")

# Change sidebar background color to #eedeff
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background-color: #eedeff;
    }
</style>
""", unsafe_allow_html=True)

df['DateKey'] = pd.to_datetime(df['DateKey'])  # Ensure datetime type

#3v. Date filter

with st.sidebar:
    # Custom CSS for date input styling
    st.markdown(
        """
        <style>
        div[data-testid="stDateInput"] {
            background-color: #eedeff !important;
            border: 2px solid #4b0082 !important;
            border-radius: 4px !important;
            padding: 8px !important;
        }
        div[data-testid="stDateInput"] input {
            background-color: #eedeff !important;
            border: 2px solid #4b0082 !important;
            border-radius: 4px !important;
            color: #4b0082 !important;
            font-weight: bold !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    selected_date = st.date_input(
        "Select a date",
        min_value=datetime.date(2007, 1, 1),
        max_value=datetime.date(2012, 12, 31),
        value=datetime.date(2007, 1, 1)
    )
    st.write("You selected:", selected_date)


#3v. Weekly Date Filter logic for extracting actuals
#actuals from start of week (Sunday) to selected date

selected_date_dt = pd.to_datetime(selected_date)
df['Year'] = pd.to_datetime(df['DateKey']).dt.year
selected_year = selected_date_dt.year
df['DateKey'] = pd.to_datetime(df['DateKey'])
selected_week = df[df['DateKey'] == selected_date_dt]['CalendarWeekLabel'].iloc[0]


right_year_week_df = df[(df['CalendarWeekLabel'] == selected_week) & (df['Year'] == selected_year)]


if ((selected_year == 2007) & (selected_week == "Week 1")):
    min_date_week_df = right_year_week_df[right_year_week_df['CalendarDayOfWeekLabel'] == "Monday"]
else:
    min_date_week_df = right_year_week_df[right_year_week_df['CalendarDayOfWeekLabel'] == "Sunday"]

max_date_week_df = right_year_week_df[right_year_week_df['CalendarDayOfWeekLabel'] == "Saturday"]


min_date = min_date_week_df['DateKey'].min()
min_date_dt = pd.to_datetime(min_date)
max_date = max_date_week_df['DateKey'].max()
max_date_dt = pd.to_datetime(max_date)


actuals_weekly_df = df[(df['DateKey'] >= min_date_dt) & (df['DateKey'] < selected_date_dt)]

#

#4v. Weekly Date Filter logic for extracting forecast
#forecast from selected date to end of week(Saturday)
#currently, this is not using api

selected_date = selected_date.strftime("%Y-%m-%d")
max_date = max_date.strftime("%Y-%m-%d")


# Construct URL with query parameters
url = f"https://salesninjaapi-752034082007.europe-west1.run.app/predict_basic?min_date={selected_date}&max_date={max_date}"
api_data = requests.get(url).json()
api_df = pd.DataFrame()
api_df['DateKey'] = api_data.keys()
api_df['DateKey'] = pd.to_datetime(api_df['DateKey'])
api_df['Forecast'] = api_data.values()
api_df['Forecast'] = api_df['Forecast'] / 1000

forecast_weekly_df = api_df[(api_df['DateKey'] >= selected_date) & (api_df['DateKey'] <= max_date)]

merged_df = actuals_weekly_df[['DateKey', 'SalesAmount']].merge(
    forecast_weekly_df, on='DateKey', how='outer'
)
grouped_df = merged_df.groupby('DateKey', as_index=False)[['SalesAmount', 'Forecast']].sum()


# Ensure DateKey is datetime and format as date string
grouped_df['DateKey'] = pd.to_datetime(grouped_df['DateKey'])
grouped_df['Date'] = grouped_df['DateKey'].dt.strftime('%Y-%m-%d')

# Set the Date as index for plotting
grouped_df.set_index('Date', inplace=True)

# Create the figure and axes
# ... previous code ...

fig, ax = plt.subplots(figsize=(10, 6))

bar_width = 0.4
x = range(len(grouped_df))

ax.bar([i - bar_width/2 for i in x], grouped_df['SalesAmount'],
       width=bar_width, label='SalesAmount', color='#FF1493')
ax.bar([i + bar_width/2 for i in x], grouped_df['Forecast'],
       width=bar_width, label='Forecast', color='#808080')

ax.set_xticks(x)
ax.set_xticklabels(grouped_df.index, rotation=45, ha='right')
ax.set_xlabel('Date')
ax.set_ylabel('Sum')
ax.legend()

# Remove only the top and right borders
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
st.markdown("<div style='margin-top: 1.5cm;'></div>", unsafe_allow_html=True)
st.pyplot(fig)
