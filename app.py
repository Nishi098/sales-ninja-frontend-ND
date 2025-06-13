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
  "private_key_id": "540c6df5f310a9a5101366f958238cca10f21f90",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDdduKMTVzOsme4\nPYdnk7PWPwACET78h8+eXHsGUIvMXc3wNuaM+ZYzznMfqlIYACbEfywJESDEqNI4\nhsp3ajF4JEoLrQxVdrIviIJQhzaYQGsDcCG84ER1mTPnOlm+6zaWxNwHXc6AB02O\n5XZPPKLVwj6EOSmbwZrhM6iTl6ANfJl7Zff8YIB2zDRfchEVW6tSY5+WmxXQslqw\n2BMODq8ecJZbhKnSd/DQIMljNvpu5LPKw6QT5aHmv8JXrkHLd4LEzQvANuIyybHC\nisMQrHfqpN0e+pbwuOfWw9v7IsdDtCKxaQfghdk+Kmwx1wIyuZsLkasaMi1FQVUr\nJBXoMi3lAgMBAAECggEAGpEPIrkKE9KTfibFZj+x8FbM47KZbXZGSe86o9qINqsf\nD/yqAgnTwqG2rjsCj5X/EI3X5LuNBIJi+3xx78rJpemmh0mKQ/tR60FTLqFVw4Rg\nwgAS0ANQOxI2h3u4td4mBYaWhXyzj6iwnV55543007MySC7KRsC+GVtW0uO6w7sw\nU79wQGEsEP7qt4iCF78yZAqjmgjYpsrMQjW3NdyazBunvEkAmsi1ryn149zYpIqy\nJwzf9mhRQqa5QvzvBi6HZxJReKYAhGz7gBJqM3/oCaWpFqQhSu3AKfDNM3Kra4Ml\nMfeiENqacPmodZIqOmCb62fidNvUTkCKErrZbDZPcQKBgQDy91mSP7pr7zv7mb90\n+WpBdc9Ko1yQPX0YtkvX88RNowoGrmVy9H1mdB9/GLRCVRi8yL7sh27rGiliLJJ5\n2ffH/TsZQVCDTFcYvIK10z2mFZB3/VTSQ542RmWdh0c53JtM61BjnfCuef9ycG0o\n2zo53Hd75IVDFl8NjsItJGKz0QKBgQDpWEBFFRmIeMfKkLoixaqy9yANx1fdpQFV\ntiJjaa87AqjbHBiZe2ztHCcorQFSpaitRQxDUriM6IKtlFIJzDaAXe3vgEKfp0zm\nkeY1zLSTcL9Zjb+oZL+c1vUFYuvMHNPT8EEZiQCQyncsrRkymly/ZUbZQrUflVbk\nfUNeWuLB1QKBgFKnkQIuyeWkGjzKnhZWLy6bvGxAzOGI+YCpq32IwRRETcneFEca\nI3ryMDDVn6UdO/AlPsZKgJJccQ2j6iSn4SJY5Hz/+jrggpS4tKLUfRl+JoqbVPq+\n5BjEtBb2CWYGxZJSTyPEfDdu61bsJkaK5fD/LmqoYClld+qt0SL4SNexAoGAK8bB\nr/QoaSD3onoKYZGh15djLRKT2mIlYPYdd3cRR8nU8d5QgyLTqZwZgJkMYXvwrCkD\nBvJS6ySHt5bW2T21j9mPVNpEJX2WgL8oaDlyOyLw0xUzupzEEeGF8C/BuvZBi0ZM\ngETilUdWmkwTKRoqgkG2y/Wm0zkKJy2U4PO+DM0CgYB75zgnDH5yD6rQcR6ssGID\nJ/7KND9juwnPFK/QrZiEM/zwsLqcwrHC4K0v0vUBvoYW6gI+z5uyytC0tdFv4bze\ntarYDrQcVpsenAngvTxn4+I2hcn7JGGuUqCw0GTK/H3ZDRCgOu1VW9tM0A9mvDHx\n2By101kDvKIuISb5F8/Yzg==\n-----END PRIVATE KEY-----\n",
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
