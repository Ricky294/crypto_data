from crypto_data.binance.candle import StreamCandle
from crypto_data.binance.schema import (
    OPEN_TIME,
    CLOSE_PRICE,
    OPEN_PRICE,
    LOW_PRICE,
    HIGH_PRICE,
    VOLUME,
    TAKER_BUY_QUOTE_ASSET_VOLUME,
    QUOTE_ASSET_VOLUME,
)
from tests import binance_stream_sample_data


def test_stream_candle():
    candle = StreamCandle(binance_stream_sample_data)

    df = candle.to_dataframe(
        columns=[OPEN_TIME, CLOSE_PRICE, OPEN_PRICE, LOW_PRICE, HIGH_PRICE, VOLUME]
    )

    columns = df.columns

    print(candle)

    assert OPEN_PRICE in columns
    assert LOW_PRICE in columns
    assert VOLUME in columns
    assert TAKER_BUY_QUOTE_ASSET_VOLUME not in columns
    assert QUOTE_ASSET_VOLUME not in columns
