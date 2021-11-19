Trading bot project.


Try it by yourself.

```python
import pandas as pd
import websocket

from crypto_data.binance.extract import get_candles
from crypto_data.binance.stream import candle_stream
from crypto_data.binance.schema import (
    COLUMNS,
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
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


def main():
    db = CandleDB(db_path="path/to/folder/binance_candle.db")

    # See all columns you can get in get_columns function
    print(COLUMNS)

    candles = get_candles(
        symbol="btcusdt",
        interval="1h",
        market="futures",   # "futures" or "spot"
        db=db,
        columns=[OPEN_TIME, OPEN_PRICE, CLOSE_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
    )

    candle_stream(
        symbol="btcusdt",
        interval="1h",
        candles=candles,
        on_open=on_open,
        on_close=on_close,
        on_candle=on_candle,
        on_candle_close=on_candle_close,
    )


if __name__ == "__main__":
    main()
