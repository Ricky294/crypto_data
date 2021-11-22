import pandas as pd

from crypto_data.binance.extract import get_candles
from crypto_data.binance.schema import (
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
)
from crypto_data.binance.stream import candle_stream
from crypto_data.shared.candle_db import CandleDB


def on_candle(candle: dict):
    print(candle)


def on_candle_close(candles: pd.DataFrame):
    print(candles.tail(n=20))


def main():

    symbol = "btcusdt"
    interval = "15m"
    market = "futures"
    db = CandleDB("binance_candles.db")

    candles_df = get_candles(
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
        candles=candles_df,
        on_candle=on_candle,
        on_candle_close=on_candle_close,
    )


if __name__ == "__main__":
    main()
