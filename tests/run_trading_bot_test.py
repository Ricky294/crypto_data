from datetime import datetime

import pandas as pd
import websocket

from tests import tests_path
from crypto_data.shared.utils import exclude_values
from crypto_data.binance.extract import get_candles, get_latest_candle_timestamp
from crypto_data.binance.schema import (
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
    COLUMNS,
)
from crypto_data.shared.candle_db import CandleDB


def on_open(_: websocket.WebSocketApp):
    print("Stream open...")


def on_close(_: websocket.WebSocketApp):
    print("Stream close...")


def on_candle(_: websocket.WebSocketApp, candle: dict):
    print(candle)


def on_candle_close(_: websocket.WebSocketApp, candles: pd.DataFrame):
    print(candles)


def test_get_candles():
    columns_to_include = [
        OPEN_TIME,
        OPEN_PRICE,
        CLOSE_PRICE,
        HIGH_PRICE,
        LOW_PRICE,
        VOLUME,
    ]

    db = CandleDB(f"{tests_path}/data/binance_candles.db")

    candles = get_candles(
        symbol="btcusdt",
        interval="1h",
        market="futures",
        columns_to_include=columns_to_include,
        db=db,
    )

    first_candle_timestamp = get_latest_candle_timestamp(
        "btcusdt", "1h", "futures", db_candles=None
    )

    now = datetime.now()
    last_candle_timestamp = int(
        datetime(now.year, now.month, now.day, now.hour - 1).timestamp()
    )

    assert candles[OPEN_TIME].iat[0] == first_candle_timestamp
    assert candles[OPEN_TIME].iat[len(candles[OPEN_TIME]) - 1] == last_candle_timestamp

    assert set(columns_to_include).issubset(candles.columns)
    assert not set(exclude_values(COLUMNS, columns_to_include)).issubset(
        candles.columns
    )


#    candle_stream(
#        symbol="btcusdt",
#        interval="1h",
#        candles=candles,
#        on_open=on_open,
#        on_close=on_close,
#        on_candle=on_candle,
#        on_candle_close=on_candle_close,
#    )
