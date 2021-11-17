import multiprocessing
import time
from datetime import datetime
from typing import Optional, List, Union

import pandas as pd
from binance import Client

from schema import MARKET_MAP
from src.crypto_data.shared.transform import (
    transform_binance_historical_candles,
    merge_candle_dataframes,
)

from schema import OPEN_TIME, COLUMNS
from src.crypto_data.shared.transform import (
    filter_binance_candle_dataframe,
    drop_rows_before,
)
from src.crypto_data.shared.candle_db import CandleDB
from src.crypto_data.shared.utils import (
    progress_bar,
    interval_in_seconds,
    to_timestamp,
)


__DOWNLOAD_LIMIT = 1000

__client = Client()


def get_missing_historical_candles(
    symbol: str,
    interval: str,
    market: str,
    latest_candle_time: int,
) -> Optional[pd.DataFrame]:
    progress_bar_thread = multiprocessing.Process(
        target=progress_bar,
        kwargs={
            "start_time": latest_candle_time,
            "end_time": int(time.time()),
            "interval": interval,
            "update_size": __DOWNLOAD_LIMIT,
            "sleep_in_seconds": 1,
        },
    )
    progress_bar_thread.start()

    optional_new_candles = get_historical_candle_dataframe(
        symbol=symbol,
        interval=interval,
        market=market,
        start_time=latest_candle_time * 1000,
        include_columns=COLUMNS[0 : len(COLUMNS) - 1],
        limit=__DOWNLOAD_LIMIT,
        remove_last_open_candle=True,
    )

    progress_bar_thread.terminate()
    return optional_new_candles


def get_latest_candle_timestamp(
    db_candles: Optional[pd.DataFrame],
    symbol: str,
    interval: str,
    market: str,
) -> int:
    if db_candles is None:
        return get_earliest_historical_candle_timestamp(
            symbol=symbol, interval=interval, market=market
        )
    return int(db_candles[OPEN_TIME].values[-1]) + interval_in_seconds(interval)


def get_candles(
    db: CandleDB,
    symbol: str,
    interval: str,
    market: str,
    columns: List[str],
    start: Optional[Union[datetime, float, int]] = None,
) -> pd.DataFrame:
    """
    Downloads the latest data from the binance api
    and stores it in a local database.

    Every time you run this function it refreshes the database
    with the latest data and returns the new dataset.
    """
    optional_db_candles = db.get_candles(
        symbol=symbol, interval=interval, market=market
    )

    optional_new_candles = get_missing_historical_candles(
        symbol=symbol,
        interval=interval,
        market=market,
        latest_candle_time=get_latest_candle_timestamp(
            symbol=symbol,
            interval=interval,
            market=market,
            db_candles=optional_db_candles,
        ),
    )

    if optional_new_candles is not None:
        db.append_candles(
            df=optional_new_candles,
            symbol=symbol,
            interval=interval,
            market=market,
        )

    candles = merge_candle_dataframes(
        db_candles=optional_db_candles,
        new_candles=optional_new_candles,
    )

    candles = filter_binance_candle_dataframe(candles, includes=columns)

    if start is not None:
        start = to_timestamp(start)
        candles = drop_rows_before(candles, start_time=start)
    return candles


def get_earliest_historical_candle_timestamp(symbol: str, interval: str, market: str):
    return int(
        __client._get_earliest_valid_timestamp(
            symbol.upper(), interval, MARKET_MAP[market]
        )
        / 1000
    )


def get_historical_candle_dataframe(
    symbol: str,
    interval: str,
    market: str,
    include_columns: List[str],
    start_time: int,
    end_time: int = None,
    remove_last_open_candle: bool = True,
    limit: int = 1000,
) -> Optional[pd.DataFrame]:
    candles = __client.get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_time,
        end_str=end_time,
        klines_type=MARKET_MAP[market],
        limit=limit,
    )

    if candles and remove_last_open_candle:
        candles.pop()

    if candles:
        return transform_binance_historical_candles(candles, include_columns)
