from ETL_Yahoo_Finance import extract, reshape_to_long,transform, load
import pytest, pandas as pd, numpy as np
from unittest.mock import MagicMock, patch

#import the functions from the main script for testing at every stage of ETL

@pytest.fixture
def tickers():
    return ['MSFT', 'AAPL', 'GOOG', 'NVDA', 'AMD', 'TM', 'INTC', 'AMZN', 'NFLX', 'META']

@pytest.fixture
def raw_data():
    return extract()

@pytest.fixture
def reshaped_data(raw_data):
    return reshape_to_long(raw_data)

@pytest.fixture
def clean_data(reshaped_data):
    return transform(reshaped_data)

# -----------------------------------------------------------------
# EXTRACT TESTS 
# -----------------------------------------------------------------

# test that database is not empty
def test_extract_not_empty(raw_data):
    assert raw_data is not None

# test that columns exist
def test_extract_columns_exist(raw_data):
    expected_columns = ['Close', 'High', 'Low', 'Open', 'Volume']
    for column in expected_columns:
        assert column in raw_data.columns

# test index is datetime
def test_extract_index_is_datetime(raw_data):
    assert raw_data.index.dtype.kind == 'M' # M is the datetime type

# test recent data exists
def test_extract_latest_data_exists(raw_data):
    assert raw_data.index.max() > pd.Timestamp("2025-01-01") # data is from 1 year ago so it should be greater than January 1, 2025

# -----------------------------------------------------------------
# RESHAPE TESTS 
# -----------------------------------------------------------------

# test to make sure data is not empty 
def test_reshape_not_empty(reshaped_data):
    assert len(reshaped_data) > 0

# test to ensure the correct column structure
def test_reshape_column_structure(reshaped_data):
    required_cols = {"date", "ticker", "open", "high", "low", "close", "volume"}
    assert required_cols.issubset(reshaped_data.columns)

# test to make sure the tickers are in the reshaped data
def test_reshape_contains_expected_tickers(reshaped_data, tickers):
    assert set(reshaped_data["ticker"].unique()) == set(tickers)

# test to check if there are no duplicates
def test_reshape_no_duplicate_date_ticker_pairs(reshaped_data):
    assert not reshaped_data.duplicated(subset=["date", "ticker"]).any()

# test to make sure date is datetime after reshaping
def test_reshape_date_is_datetime(reshaped_data):
    assert pd.api.types.is_datetime64_any_dtype(reshaped_data["date"])

# -----------------------------------------------------------------
# TRANSFORM TESTS 
# -----------------------------------------------------------------

# test to ensure data is not empty after transformation
def test_transform_not_empty(clean_data):
    assert len(clean_data) > 0

# test to ensure the correct column structure
def test_transform_column_structure(clean_data):
    required_cols = {
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "ma_7",
        "volatility_7",
        "price_range",
        "normalized",
    }
    assert required_cols.issubset(clean_data.columns)

# test to make sure the tickers are in the transformed data
def test_transform_contains_expected_tickers(clean_data, tickers):
    assert set(clean_data["ticker"].unique()) == set(tickers)

# test to ensure there are no duplicates
def test_transform_no_duplicate_date_ticker_pairs(clean_data):
    assert not clean_data.duplicated(subset=["date", "ticker"]).any()

# test to check the math is correct for the daily return
def test_transform_daily_return(clean_data):
    for ticker, group in clean_data.groupby("ticker"):
        group = group.sort_values("date").copy()

        expected = group["close"].pct_change()
        actual = group["daily_return"]

        mask = expected.notna()
        expected = expected[mask]
        actual = actual[mask]

        assert np.allclose(expected, actual, atol=1e-6)

# test to check the math for the seven day moving average
def test_transform_seven_day_moving_avg(clean_data):
    for ticker, group in clean_data.groupby("ticker"):
        group = group.sort_values("date").copy()

        expected = group["close"].rolling(7).mean()
        actual = group["ma_7"]

        mask = expected.notna()
        expected = expected[mask]
        actual = actual[mask]

        assert np.allclose(expected, actual, atol=1e-6)

# test to check the math for the seven day volatility
def test_transform_seven_day_volatility(clean_data):
    for ticker, group in clean_data.groupby("ticker"):
        group = group.sort_values("date").copy()

        expected = group["daily_return"].rolling(7).std()
        actual = group["volatility_7"]

        mask = expected.notna()
        expected = expected[mask]
        actual = actual[mask]

        assert np.allclose(expected, actual, atol=1e-6)

# test to check the math for the price range
def test_transform_price_range(clean_data):
    expected = clean_data["high"] - clean_data["low"]
    actual = clean_data["price_range"]

    assert np.allclose(expected, actual, atol=1e-6)

# test to check the math for the normalization
def test_transform_normalization(clean_data):
    for ticker, group in clean_data.groupby("ticker"):
        group = group.sort_values("date").copy()

        actual = group["normalized"]
        expected = group["close"] / (group["close"] / group["normalized"]).iloc[0]

        assert np.allclose(expected, actual, atol=1e-6)

# test to make sure the transformed data is sorted by tickers and that the individual tickers are sorted by date
def test_transform_is_sorted(clean_data):
    expected = clean_data.sort_values(["ticker", "date"]).reset_index(drop=True)
    actual = clean_data.reset_index(drop=True)

    pd.testing.assert_frame_equal(actual, expected)

# test to ensure there are no NaN values
def test_transform_no_nans(clean_data):
    assert not clean_data.isna().any().any()


# -----------------------------------------------------------------
# LOAD TESTS 
# -----------------------------------------------------------------

@pytest.fixture
def sample_clean_data():
    return pd.DataFrame({
        "date": pd.to_datetime(["2025-04-23", "2025-04-24"]),
        "ticker": ["AAPL", "AAPL"],
        "open": [205.10, 204.00],
        "high": [207.09, 207.92],
        "low": [201.91, 202.05],
        "close": [203.71, 207.46],
        "volume": [52929200, 47311000],
        "daily_return": [0.024332, 0.018426],
        "ma_7": [198.19, 199.02],
        "volatility_7": [0.026337, 0.025976],
        "price_range": [5.17, 5.86],
        "normalized": [1.03, 1.05],
    })

# Make sure an empty dataframe is not loaded into the db
def test_load_empty_dataframe_raises():
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="empty"):
        load(empty_df)

# create mock objects with @patch annotation
@patch("ETL_Yahoo_Finance.execute_values")
@patch("ETL_Yahoo_Finance.psycopg2.connect")
def test_load_success(mock_connect, mock_execute_values, sample_clean_data):
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor

    load(sample_clean_data)

    # DB connection created
    mock_connect.assert_called_once()

    # cursor created
    mock_connection.cursor.assert_called_once()

    # table creation query executed
    executed_queries = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS market_data" in q for q in executed_queries)
    assert any("CREATE INDEX IF NOT EXISTS idx_market_data_ticker_date" in q for q in executed_queries)

    # bulk insert called
    mock_execute_values.assert_called_once()
    args = mock_execute_values.call_args[0]
    inserted_records = args[2]
    assert len(inserted_records) == len(sample_clean_data)

    # committed
    mock_connection.commit.assert_called_once()

    # closed
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()


@patch("ETL_Yahoo_Finance.execute_values")
@patch("ETL_Yahoo_Finance.psycopg2.connect")
def test_load_rolls_back_on_insert_failure(mock_connect, mock_execute_values, sample_clean_data):
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor

    mock_execute_values.side_effect = Exception("insert failed")

    with pytest.raises(Exception, match="insert failed"):
        load(sample_clean_data)

    mock_connection.rollback.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()