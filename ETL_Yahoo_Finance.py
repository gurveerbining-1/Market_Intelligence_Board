import os
import yfinance as yf
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

def extract():
    # extract data from yfinance
    data = yf.download(['MSFT', 'AAPL', 'GOOG', 'NVDA', 'AMD', 'TM', 'INTC', 'AMZN', 'NFLX', 'META'], period='1y')
    
    return(data)
    
    #Close: The final traded price of the period, often considered the most significant price.
    #High: The maximum price reached during the period.
    #Low: The minimum price reached during the period.
    #Open: The first traded price of the period.
    #Volume: The total number of shares or contracts traded, indicating market activity. 

def reshape_to_long(df):
    #Convert yfinance's wide MultiIndex column format into a tidy long format.
    #Output columns:
    #date, ticker, open, high, low, close, volume
       
    if df.empty:
        raise ValueError("reshape_to_long received an empty DataFrame.")

    # Stack the ticker level into rows
    long_df = (
        df.stack(level=1, future_stack=True)
        .reset_index()
        .rename(columns={
            'Date': 'date',
            'Ticker': 'ticker',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
    )

    # Clean up types/order
    long_df['date'] = pd.to_datetime(long_df['date'])
    long_df['ticker'] = long_df['ticker'].astype(str)

    long_df = long_df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
    long_df = long_df.sort_values(['ticker', 'date']).reset_index(drop=True)

    return long_df



def transform(df):
    df = df.copy()

    # Ensure correct types
    df['date'] = pd.to_datetime(df['date'])
    df['ticker'] = df['ticker'].astype(str)
    df.columns.name = None # this was so that Price is not listed as the column name

    # Sort so rolling calculations happen in the right order
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)

    # Handle missing values within each ticker only
    price_cols = ['open', 'high', 'low', 'close', 'volume']
    df[price_cols] = df.groupby('ticker')[price_cols].ffill()

    # --- New Features ---

    # percentage change in close from previous trading day
    df['daily_return'] = df.groupby('ticker')['close'].pct_change()

    # 7-day moving average of close
    df['ma_7'] = (
        df.groupby('ticker')['close']
        .transform(lambda s: s.rolling(window=7, min_periods=7).mean())
    )

    # 7-day rolling volatility based on daily returns
    df['volatility_7'] = (
        df.groupby('ticker')['daily_return']
        .transform(lambda s: s.rolling(window=7, min_periods=7).std())
    )

    # range calculation
    df['price_range'] = df['high'] - df['low']

    # normalized price relative to each ticker's first close
    df['normalized'] = (
        df.groupby('ticker')['close']
        .transform(lambda s: s / s.iloc[0])
    )

    # remove rows where rolling metrics are not yet available
    df = df.dropna().reset_index(drop=True)

    return df


def load(transformed_df):
    # write to PostgreSQL
    
    #with open("output.txt", "w", encoding="utf-8") as f:
    #    f.write(transformed_df.to_string())
    if transformed_df.empty:
        raise ValueError("Transformed dataframe is empty. Nothing to load.")
    
    # set these to None in case an empty df is passed and the code in the finally statement starts to run
    connection = None
    cur = None
    try:
        # check if transformed data frame is empty
        
        #print(transformed_df.head())
        # 1. Connect to database
        connection = psycopg2.connect(
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            host = os.getenv("DB_HOST"),
            database = os.getenv("DB_NAME")
        )

        # Create a cursor for database operations
        cur = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS market_data (
            date DATE NOT NULL,
            ticker TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume BIGINT NOT NULL,
            daily_return DOUBLE PRECISION NOT NULL,
            ma_7 DOUBLE PRECISION NOT NULL,
            volatility_7 DOUBLE PRECISION NOT NULL,
            price_range DOUBLE PRECISION NOT NULL,
            normalized DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (date, ticker)
        );
        """

        # Execute a query
        cur.execute(create_table_query)

        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_market_data_ticker_date
        ON market_data (ticker, date);
        """
        cur.execute(create_index_query)

        records = [
            (
                row.date.date(),
                row.ticker,
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                int(row.volume),
                float(row.daily_return),
                float(row.ma_7),
                float(row.volatility_7),
                float(row.price_range),
                float(row.normalized),
            )
            for row in transformed_df.itertuples(index=False)
        ]

        # query allows for conflicting data to be updated and data that does not exist to be added into the db
        insert_query = """
        INSERT INTO market_data (
            date,
            ticker,
            open,
            high,
            low,
            close,
            volume,
            daily_return,
            ma_7,
            volatility_7,
            price_range,
            normalized
        )
        VALUES %s
        ON CONFLICT (date, ticker) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            ma_7 = EXCLUDED.ma_7,
            volatility_7 = EXCLUDED.volatility_7,
            price_range = EXCLUDED.price_range,
            normalized = EXCLUDED.normalized;
        """

        execute_values(cur, insert_query, records) # helper function that allows the user to insert many rows at once into PostgreSQL efficiently
        connection.commit()

        print(f"Loaded {len(records)} rows into market_data.")

    except Exception as error:
        if connection:
            connection.rollback()
        print(f"Error loading data into database: {error}")
        raise
    

    finally:
        if cur:
            cur.close()
        if connection:
            connection.close()    

def main():
    raw_data = extract()
    reshaped_data = reshape_to_long(raw_data)   
    clean_data = transform(reshaped_data) 
    load(clean_data)

if __name__ == "__main__":
    main()