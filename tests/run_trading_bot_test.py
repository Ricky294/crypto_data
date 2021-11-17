from tests import tests_path
from crypto_data.binance.extract import get_candles
from crypto_data.binance.schema import (
    OPEN_TIME,
    OPEN_PRICE,
    CLOSE_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
)
from crypto_data.shared.candle_db import CandleDB


def test_init_trading_bot():
    db = CandleDB(f"{tests_path}/data", "binance")

    candles = get_candles(
        symbol="btcusdt",
        interval="1h",
        market="futures",
        columns=[OPEN_TIME, OPEN_PRICE, CLOSE_PRICE, HIGH_PRICE, LOW_PRICE, VOLUME],
        db=db,
    )

    """start_stream(
        symbol="btcusdt",
        interval="1h",
        candles=candles,
    )"""
