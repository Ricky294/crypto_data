from typing import Dict

import pandas as pd

from crypto_data.binance.pd.extract import get_candles
from crypto_data.binance.schema import (
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
)
from crypto_data.binance.pd.stream import candle_stream, candle_multi_stream
from crypto_data.binance.candle import StreamCandle
from crypto_data.shared.candle_db import CandleDB


def on_candle(candle: StreamCandle):
    print(candle)


def on_candle_close(candles: pd.DataFrame):
    print(candles.tail(n=20))


def on_multi_candle_close(candle: StreamCandle, candles: Dict[str, pd.DataFrame]):
    print(f"Symbol of latest candle: {candle.symbol}")
    print(candles[candle.symbol].tail(n=20))


def multi_symbol():

    btcusdt = "btcusdt"
    ethusdt = "ethusdt"

    interval = "15m"
    market = "futures"
    db = CandleDB("binance_candles.db")

    btcusdt_df = get_candles(
        symbol=btcusdt,
        interval=interval,
        market=market,
        db=db,
        columns=[
            OPEN_TIME,
            OPEN_PRICE,
            CLOSE_PRICE,
            HIGH_PRICE,
            LOW_PRICE,
            VOLUME,
        ],
    )

    ethusdt_df = get_candles(
        symbol=ethusdt,
        interval=interval,
        market=market,
        db=db,
        columns=[
            OPEN_TIME,
            OPEN_PRICE,
            CLOSE_PRICE,
            HIGH_PRICE,
            LOW_PRICE,
            VOLUME,
        ],
    )

    candle_multi_stream(
        symbol_candles={btcusdt: btcusdt_df, ethusdt: ethusdt_df},
        interval=interval,
        market=market,
        on_candle=on_candle,
        on_candle_close=on_multi_candle_close,
    )


def single_symbol():

    symbol = "btcusdt"
    interval = "15m"
    market = "futures"
    db = CandleDB("binance_candles.db")

    candle_df = get_candles(
        symbol=symbol,
        interval=interval,
        market=market,
        db=db,
        columns=[
            OPEN_TIME,
            OPEN_PRICE,
            CLOSE_PRICE,
            HIGH_PRICE,
            LOW_PRICE,
            VOLUME,
        ],
    )

    candle_stream(
        symbol=symbol,
        interval=interval,
        market=market,
        candles=candle_df,
        on_candle=on_candle,
        on_candle_close=on_candle_close,
    )


if __name__ == "__main__":
    single_symbol()
    # multi_symbol()
