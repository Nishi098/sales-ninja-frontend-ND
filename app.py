import streamlit as st
import pandas as pd
from dateutil.parser import parse
import requests
import datetime
import matplotlib.pyplot as plt
import datetime
import calendar
import pandas_gbq
from google.oauth2 import service_account

#I need a dataset with dates for each day
#I need kumar's table to have data for each date
#I need my 0.1% sample to have data from each date
#I probably need my data to be multiplied by 1000


# Load credentials from Streamlit secrets
credentials_dict = {
  "type": "service_account",
  "project_id": "nodal-clock-456815-g3",
  "private_key_id": "d7b300b4e7bcf35c62ac237216cc603bec9bea4f",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCooaD7zWERbRfA\nVYLapS5etD32XoQRcZGhoFs9zVGBfCHVJ9K1thQnzvCfAmQp7j+e9u+DNlFOINKX\nCzCjTRtZUIfxQ4cS2wZRw4ayFJPbmwVqSgCJSAJDEyXju4PQuyBDrCZbEaGNABdH\nLbKhEv31ypun1tT7rImYP8lDspARzoMlIoAc9gIcljTqtxroqPgEKQLl9vxfLOhM\nOBlypWwNLDQWejexB2CdcMFZ4hCvqx9A0bMmsd1CzpCybcTeTsjSx3jlzsb9QYsz\nfSzjNMQcu0CvkUPMIPazIf1q9siblDLAM6fHecrsAnLxVq4YoQErTtBmzwqf1evf\nHbJG6vDnAgMBAAECggEAOyQIahczAmIQ0OcMj/MbiqJLEuM+DUIX0agEJ+4gtjlb\nQj8HsqvrcLSuhg3YJC6HCJDRGmML68suIEQP6E3MGCxaqP5GWpIPKtDYpg76divt\nn8PZYUwsYurNwItMpQFjxOgUwZ6y2lkcUN3RhpU7FR9cLOZ25tcxoIBKbWAsD9ob\nB4gaSsSKTEBOegKkmJociBvyx1Rj/Gu6wlfTWIqbjVJqFet/g3gNz+OfNe2gZsQr\n01NYNWvwtTaEcd2J6mEX2qHNSGPp2i1MJ0Osmip8LbFULMXUqPShyjE3iX/nt/5L\nx/LOd7TvOcykSlMnMErg3Ja/IH9QYt6KQHFEjg0YoQKBgQDsYoAFkoiKcPVnsEca\nd1RHHeHBhUByfIW0omZF/HHC+XtuGysrK3a8/c4DOA255BcFJpjbbJxHHSLBSSGj\n6h0tcOnPsAJqrLV2mEecOlmm7AtUze8xNerj4mDWEgw1N7UypWykH34xzlwD5EMR\nwl02duQXQgftBsTQktvdWfwY4QKBgQC2n9lzsqpQNJiGDaUbMulXkLHTDxeiuwtR\nmfEJui1a96JpejPRQGXLP1fn4GgxCIIAdRVeMqafkt7dYS2Rk3o0//TAOX0C3tJS\n5QABE5yy4GY2RUMMzZBEDA7zHRE1+MYo5QN6P0xjTkp+oiiYLAdsJko8rKlM9y18\nGREka0TaxwKBgQCcLLvOcrC5XsYUOnfPuZU54zk7ZTFeMn0YCc+uX4o9uhzdcx/D\nRtUNKlaI8+jFrzeyVHzbQ9fAeDR1JT8Pj+a1Fgu0BuKh3feKIjP1uOtwiDU4U1K7\n3ZaR9wfph0T/iA20J20uxgvXFjLe81mIizSQfl5WK28XH8i60Lxoc0JGoQKBgHoy\ng4JTLnr0dopcXvnQGyqoexRKqPoORgiYBR0JIogX4ujJCBsgB/zzqZJSKeWFV9eO\ngHnDUpK757wh5ifekoscKVzmGqvtBLbK3DHcOaHHXR07Qx4x/jJKD0bFUFrY119N\nvgHykN5x6I7LWnZYH69R/6KRtcb2Lc03yKyhyjTJAoGAYTyYfCniqxCuriDyV5D1\n2WSwm7Gyfh2NEy3NPBjihuKgDDdC5+zHZ9Z5lMkQ2lU7VZcRsX10RwxSk81T+0Yk\nz+d7koujLWS9fV90Eq2MaDZJ83DwgGj8sINW73WjQi1nPjbU3I39LvjPybZ8zKrn\nfb6wFCiPOzpqnplrbm1rlBY=\n-----END PRIVATE KEY-----\n",
  "client_email": "le-wagon-data-bootcamp@nodal-clock-456815-g3.iam.gserviceaccount.com",
  "client_id": "108534482050987661068",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/le-wagon-data-bootcamp%40nodal-clock-456815-g3.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
credentials = service_account.Credentials.from_service_account_info(credentials_dict)




#1f. Load dull data for actuals
min_date_load = parse("2007-01-01").strftime('%Y-%m-%d')  # e.g. '2007-01-01'
max_date_load = parse("2009-12-31").strftime('%Y-%m-%d')  # e.g. '2009-12-31'

query = f"""
    select * from `dashboard_data.dashboard_merged_data_1`
    WHERE DateKey BETWEEN '{min_date_load}' AND '{max_date_load}'
    ORDER BY DateKey
"""


# Query BigQuery using pandas_gbq
df_full = pandas_gbq.read_gbq(
    query,
    project_id='nodal-clock-456815-g3',
    credentials=credentials
)
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


#5v. Plotting the weekly development graph
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
