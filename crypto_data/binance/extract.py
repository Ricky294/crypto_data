import multiprocessing
import time
from datetime import datetime

from typing import Optional, List, Union

import pandas as pd
from binance.client import Client

from crypto_data.binance.transform import transform_binance_historical_candles
from crypto_data.binance.schema import OPEN_TIME, COLUMNS, MARKET_MAP
from crypto_data.enum.market import Market
from crypto_data.shared.transform import (
    filter_dataframe_by_columns,
    safe_merge_dataframes,
)
from crypto_data.shared.candle_db import CandleDB
from crypto_data.shared.utils import (
    progress_bar,
    interval_in_seconds,
    to_timestamp,
)


def _get_missing_historical_candles(
    symbol: str,
    interval: str,
    market: str,
    latest_candle_time: int,
) -> Optional[pd.DataFrame]:
    download_limit = 1000

    progress_bar_thread = multiprocessing.Process(
        target=progress_bar,
        kwargs={
            "start_time": latest_candle_time,
            "end_time": int(time.time()),
            "interval": interval,
            "update_size": download_limit,
            "sleep_in_seconds": 1,
        },
    )
    progress_bar_thread.start()

    optional_new_candles = _get_historical_candle_dataframe(
        symbol=symbol,
        interval=interval,
        market=market,
        start_time=latest_candle_time * 1000,
        include_columns=COLUMNS[0 : len(COLUMNS) - 1],
        limit=download_limit,
        remove_last_open_candle=True,
    )

    progress_bar_thread.terminate()
    return optional_new_candles


def _get_latest_candle_timestamp(
    symbol: str,
    interval: str,
    market: str,
    db_candles: Optional[pd.DataFrame],
) -> int:
    if db_candles is None:
        return _get_earliest_historical_candle_timestamp(
            symbol=symbol, interval=interval, market=market
        )
    return int(db_candles[OPEN_TIME].values[-1]) + interval_in_seconds(interval)


def get_candles(
    symbol: str,
    interval: str,
    market: Union[Market, str],
    db: CandleDB,
    columns: List[str],
    start_time: Union[datetime, float, int] = None,
    limit: int = None,
) -> pd.DataFrame:
    """
    Downloads the latest data from the binance api
    and stores it in a local database.

    Every time you run this function it refreshes the database
    with the latest data and returns the new dataset.
    """

    if start_time is not None and limit is not None:
        raise ValueError("max_limit and start_time params are both not None")

    optional_db_candles = db.get_candles(
        symbol=symbol, interval=interval, market=str(market)
    )

    optional_new_candles = _get_missing_historical_candles(
        symbol=symbol,
        interval=interval,
        market=str(market),
        latest_candle_time=_get_latest_candle_timestamp(
            symbol=symbol,
            interval=interval,
            market=str(market),
            db_candles=optional_db_candles,
        ),
    )

    if optional_new_candles is not None:
        db.append_candles(
            df=optional_new_candles,
            symbol=symbol,
            interval=interval,
            market=str(market),
        )

    candles = safe_merge_dataframes(
        append_to_df=optional_db_candles,
        other_df=optional_new_candles,
    )

    candles = filter_dataframe_by_columns(
        candles,
        all_columns=COLUMNS[0 : len(COLUMNS) - 1],
        columns_to_include=columns,
    )

    if start_time is not None:
        start = to_timestamp(start_time)
        return candles[candles[OPEN_TIME] >= start]
    elif limit is not None:
        return candles.tail(limit)
    return candles


def _get_earliest_historical_candle_timestamp(symbol: str, interval: str, market: str):
    return int(
        Client()._get_earliest_valid_timestamp(
            symbol.upper(), interval, MARKET_MAP[market.upper()]
        )
        / 1000
    )


def _get_historical_candle_dataframe(
    symbol: str,
    interval: str,
    market: str,
    include_columns: List[str],
    start_time: int,
    end_time: int = None,
    remove_last_open_candle: bool = True,
    limit: int = 1000,
) -> Optional[pd.DataFrame]:
    candles = Client().get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_time,
        end_str=end_time,
        klines_type=MARKET_MAP[market.upper()],
        limit=limit,
    )

    if candles and remove_last_open_candle:
        candles.pop()

    if candles:
        return transform_binance_historical_candles(candles, include_columns)
