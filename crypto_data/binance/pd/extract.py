import multiprocessing
import time
from datetime import datetime

from typing import Optional, List, Union

import pandas as pd
from binance.client import Client

from crypto_data.log import logger
from crypto_data.binance.pd.transform import transform_binance_historical_candles
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


class Limit:
    def __init__(self, type: str, value: Union[datetime, int, float]):
        self.type = type
        self.value = value

    @classmethod
    def datetime(cls, value: Union[datetime, int, float]):
        return cls(type="datetime", value=value)

    @classmethod
    def max_records(cls, value: int):
        return cls(type="max_records", value=value)


def get_candles(
    symbol: str,
    interval: str,
    market: Union[Market, str],
    db: CandleDB,
    columns: List[str],
    download_missing: bool = True,
    limit: Limit = None,
) -> pd.DataFrame:
    """
    Downloads the latest data from the binance api
    and stores it in a local database.

    Every time you run this function it refreshes the database
    with the latest data and returns the new dataset.
    """

    market = str(market).upper()
    table_name = f"{symbol}_{market}_{interval}".lower()

    logger.info(f"Attempting to read data from (database: {db.db_path!r}, table: {table_name!r}).")
    optional_db_candles = db.get_candles(table_name=table_name)
    if optional_db_candles is None:
        logger.info(f"Table {table_name!r} not exists/empty.")
    else:
        logger.info(f"{optional_db_candles.shape} read from table {table_name!r}.")

    if optional_db_candles is None and not download_missing:
        raise ValueError(
            "Parameter download_missing is set to False, while database table is empty. "
            "To solve this issue set download_missing to True."
        )

    optional_new_candles = None
    if download_missing:
        latest_candle_time = _get_latest_candle_timestamp(
            symbol=symbol,
            interval=interval,
            market=market,
            db_candles=optional_db_candles,
        )

        download_limit = 1000

        message = "Downloading" if optional_db_candles is None else "Updating"
        message += " candles. (Note: The below progress bar is just an estimate, not necessarily accurate.)"

        logger.info(message)
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

        optional_new_candles = _download_historical_candle_dataframe(
            symbol=symbol,
            interval=interval,
            market=market,
            start_time=latest_candle_time * 1000,
            include_columns=COLUMNS[0 : len(COLUMNS) - 1],
            limit=download_limit,
            remove_last_open_candle=True,
        )

        progress_bar_thread.terminate()
        logger.info("Data downloaded successfully.")
    else:
        logger.info("Skipping downloading new candles.")

    if optional_new_candles is not None:
        db.append_candles(
            df=optional_new_candles,
            table_name=table_name,
        )

    candles = safe_merge_dataframes(
        append_to_df=optional_db_candles,
        other_df=optional_new_candles,
    )

    candles = filter_dataframe_by_columns(
        candles,
        all_columns=COLUMNS[0:len(COLUMNS) - 1],
        columns_to_include=columns,
    )

    if limit is not None:
        if limit.type == "datetime":
            start = to_timestamp(limit.value)
            return candles[candles[OPEN_TIME] >= start]
        if limit.type == "max_records":
            return candles.tail(limit.value)
    return candles


def _get_earliest_historical_candle_timestamp(symbol: str, interval: str, market: str):
    return int(
        Client()._get_earliest_valid_timestamp(
            symbol.upper(), interval, MARKET_MAP[market.upper()]
        )
        / 1000
    )


def _download_historical_candle_dataframe(
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
