import os
import yfinance as yf
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

load_dotenv()

def get_data():
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
    )

    query = "SELECT * FROM market_data"
    df = pd.read_sql(query, engine)
    return df
st.title("Market Intelligence Dashboard")

df = get_data()

st.sidebar.header("Filters")

all_tickers = sorted(df["ticker"].unique())
selected_tickers = st.sidebar.multiselect(
    "Select ticker(s)",
    options=all_tickers,
    default=["AAPL"]
)

min_date = df["date"].min()
max_date = df["date"].max()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) != 2:
    st.warning("Please select a valid start and end date.")
    st.stop()

start_date, end_date = date_range

filtered_df = df[
    (df["ticker"].isin(selected_tickers)) &
    (df["date"] >= start_date) &
    (df["date"] <= end_date)
].copy()

if filtered_df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# -----------------------------
# Metrics
# -----------------------------
st.subheader("Latest Metrics")

latest_per_ticker = (
    filtered_df.sort_values("date")
    .groupby("ticker", as_index=False)
    .tail(1)
    .sort_values("ticker")
)

if len(selected_tickers) == 1:
    latest_row = latest_per_ticker.iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Latest Close", f"{latest_row['close']:.2f}")
    col2.metric("Daily Return", f"{latest_row['daily_return'] * 100:.2f}%")
    col3.metric("7-Day Moving Avg", f"{latest_row['ma_7']:.2f}")
    col4.metric("7-Day Volatility", f"{latest_row['volatility_7'] * 100:.2f}%")
    col5.metric("Normalized", f"{latest_row['normalized']:.2f}x")

else:
    st.dataframe(
        latest_per_ticker[
            ["ticker", "date", "close", "daily_return", "ma_7", "volatility_7", "normalized"]
        ].rename(columns={
            "ticker": "Ticker",
            "date": "Date",
            "close": "Close",
            "daily_return": "Daily Return",
            "ma_7": "7-Day MA",
            "volatility_7": "7-Day Volatility",
            "normalized": "Normalized"
        }),
        use_container_width=True
    )

# -----------------------------
# Chart 1: Close Price
# -----------------------------
st.subheader("Close Price Over Time")

fig_close = px.line(
    filtered_df,
    x="date",
    y="close",
    color="ticker",
    title="Closing Price"
)
st.plotly_chart(fig_close, use_container_width=True)

# -----------------------------
# Chart 2: Daily Return Distribution
# -----------------------------

# how much the stock changed from one day to the next (in % terms)
# count is the number of days that fall within this % range
filtered_df["daily_return_pct"] = filtered_df["daily_return"] * 100

st.subheader("Daily Return Distribution Over Time")
fig_hist = px.histogram(
    filtered_df,
    x=filtered_df["daily_return_pct"],
    color="ticker",
    nbins=50,
    title="Daily Return Distribution"
)
st.plotly_chart(fig_hist, use_container_width=True)

# -----------------------------
# Chart 3: Volume
# -----------------------------

st.subheader("Trading Volume Over Time")

fig_volume = px.bar(
    filtered_df,
    x="date",
    y="volume",
    color="ticker",
    barmode="group",
    title="Trading Volume"
)

st.plotly_chart(fig_volume, use_container_width=True)
