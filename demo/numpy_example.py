import logging

import numpy as np

from crypto_data.binance.pd.extract import get_candles
from crypto_data.binance.schema import (
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
)
from crypto_data.binance.np.stream import candle_stream
from crypto_data.binance.candle import StreamCandle
from crypto_data.shared.candle_db import CandleDB


def on_candle(candle: StreamCandle):
    ...


def on_candle_close(candles: np.ndarray):
    print(candles[-20:])


def single_symbol():
    logging.basicConfig(level=logging.INFO)

    symbol = "btcusdt"
    interval = "1h"
    market = "futures"
    db = CandleDB("binance_candles.db")

    columns = [
        OPEN_TIME,
        OPEN_PRICE,
        CLOSE_PRICE,
        HIGH_PRICE,
        LOW_PRICE,
        VOLUME,
    ]

    candle_df = get_candles(
        symbol=symbol,
        interval=interval,
        market=market,
        db=db,
        columns=columns,
        download_missing=True,
    )

    candle_arr = candle_df.to_numpy()

    candle_stream(
        symbol=symbol,
        interval=interval,
        market=market,
        columns=columns,
        candles=candle_arr,
        on_candle=on_candle,
        on_candle_close=on_candle_close,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
    )
    single_symbol()
