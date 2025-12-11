import streamlit as st
import pandas as pd
import altair as alt
import boto3

def load_data_from_s3(bucket: str, key: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS"]["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS"].get("AWS_REGION", "us-east-1")
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'], parse_dates=["date"])
    return df

s3_bucket = "weather-etl-emily-2025"
s3_key = "combined/boston_weather_combined.csv"
df = load_data_from_s3(s3_bucket, s3_key)

st.title("Boston Weather Dashboard 🌤️")

current_weather = df[df['source'] == 'api'].sort_values('date', ascending=False).head(1)

if not current_weather.empty:
    temp = current_weather.iloc[0]['value_c']
    flag = current_weather.iloc[0]['daily_flag']
    dt = current_weather.iloc[0]['date']
    
    st.metric(
        label=f"Current Temperature ({dt})",
        value=f"{temp:.1f}°C",
        delta=f"{flag}"
    )

years = sorted(df['year'].dropna().unique())
selected_year = st.sidebar.selectbox("Select Year", years)

months = list(range(1, 13))
selected_month = st.sidebar.selectbox(
    "Select Month (optional)",
    [0] + months,
    format_func=lambda x: "All" if x == 0 else str(x)
)

filtered_df = df[df['year'] == selected_year]
if selected_month != 0:
    filtered_df = filtered_df[filtered_df['month'] == selected_month]

st.subheader(f"Weather Data for {selected_year}" + (f"-{selected_month}" if selected_month != 0 else ""))
st.dataframe(filtered_df.reset_index(drop=True))

if not filtered_df.empty:
    temp_chart = alt.Chart(filtered_df).transform_fold(
        fold=["TMAX", "TMIN"],
        as_=["Type", "Temperature"]
    ).mark_line(point=True).encode(
        x="date:T",
        y="Temperature:Q",
        color="Type:N",
        tooltip=["date:T", "Type:N", "Temperature:Q"]
    ).properties(
        width=700,
        height=400,
        title="Daily Max & Min Temperature"
    )
    st.altair_chart(temp_chart, use_container_width=True)

if not filtered_df.empty:
    flag_counts = filtered_df['daily_flag'].value_counts().reset_index()
    flag_counts.columns = ['Daily Flag', 'Count']

    flag_chart = alt.Chart(flag_counts).mark_bar().encode(
        x='Daily Flag:N',
        y='Count:Q',
        color='Daily Flag:N',
        tooltip=['Daily Flag', 'Count']
    ).properties(
        width=400,
        height=300,
        title="Daily Flag Counts"
    )
    st.altair_chart(flag_chart, use_container_width=True)

if 'PRCP' in filtered_df.columns:
    prcp_chart = alt.Chart(filtered_df).mark_bar().encode(
        x='date:T',
        y='PRCP:Q',
        tooltip=['date:T', 'PRCP:Q']
    ).properties(
        width=700,
        height=300,
        title="Daily Precipitation (mm)"
    )
    st.altair_chart(prcp_chart, use_container_width=True)
