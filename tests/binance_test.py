from crypto_data.binance.schema import (
    OPEN_TIME,
    VOLUME,
    CLOSE_TIME,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
)

from crypto_data.binance.pd.transform import (
    transform_binance_historical_candles,
    transform_binance_stream_candle,
    append_binance_streaming_data,
)
from tests import binance_historical_sample_data, binance_stream_sample_data


def test_binance_lists_to_dataframe():

    df = transform_binance_historical_candles(
        binance_historical_sample_data,
        [OPEN_TIME, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
    )

    columns = df.columns
    assert OPEN_TIME in columns
    assert VOLUME in columns
    assert CLOSE_TIME not in columns
    assert len(columns) == 5

    assert df.shape == (20, 5)

    assert df[OPEN_TIME][0] == 1636151460


def test_append_streaming_data():

    historical_df = transform_binance_historical_candles(
        binance_historical_sample_data,
        [OPEN_TIME, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
    )

    candle_df = transform_binance_stream_candle(
        binance_stream_sample_data["k"],
        [OPEN_TIME, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
    )

    appended_df = historical_df.append(candle_df)

    assert appended_df.shape == (21, 5)


def test_append_binance_streaming_data():

    historical_df = transform_binance_historical_candles(
        binance_historical_sample_data,
        [OPEN_TIME, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
    )

    outer_fun = append_binance_streaming_data(
        historical_df,
        lambda candle: print(candle),
        lambda candle: print(candle.tail()),
    )

    outer_fun(stream_data=binance_stream_sample_data)
    outer_fun(stream_data=binance_stream_sample_data)
