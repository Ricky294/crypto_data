import multiprocessing
import pathlib
import time
from datetime import datetime
from typing import Optional, List, Union, Callable

import pandas as pd
import websocket

from src.trading_bot.broker.binance_.broker import BinanceBroker
from src.trading_bot.broker.binance_.schema import OPEN_TIME, COLUMNS
from src.trading_bot.broker.binance_.transform import (
    append_binance_streaming_data,
    filter_binance_candle_dataframe,
    drop_rows_before,
)
from src.trading_bot.db.candle_db import CandleDB
from src.trading_bot.utils import (
    read_json,
    read_yaml,
    progress_bar,
    interval_in_seconds,
    to_timestamp,
)

__DOWNLOAD_LIMIT = 1000


def get_missing_historical_candles(
    symbol: str,
    interval: str,
    market: str,
    broker: BinanceBroker,
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

    optional_new_candles = broker.get_historical_candle_dataframe(
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
    broker: BinanceBroker,
) -> int:
    if db_candles is None:
        return broker.get_earliest_historical_candle_timestamp(
            symbol=symbol, interval=interval, market=market
        )
    return int(db_candles[OPEN_TIME].values[-1]) + interval_in_seconds(interval)


def merge_candle_dataframes(
    db_candles: Optional[pd.DataFrame],
    new_candles: Optional[pd.DataFrame],
) -> pd.DataFrame:
    if db_candles is None and new_candles is None:
        raise ValueError("At least one of the dataframes must not be None.")
    if new_candles is None:
        return db_candles
    if db_candles is None:
        return new_candles

    return db_candles.append(new_candles, ignore_index=True)


def read_config(path: str) -> dict:
    extension = pathlib.Path(path).suffix
    if "yaml" in extension:
        return read_yaml(path)
    if "json" in extension:
        return read_json(path)


class TradingBot:
    def __init__(
        self,
        broker_name: str,
        symbol: str,
        interval: str,
        market: str,
        columns: List[str],
        start: Union[datetime, int, float],
        api_key: str,
        api_secret: str,
    ):
        self.broker_name = broker_name
        self.symbol = symbol
        self.interval = interval
        self.market = market
        self.columns = columns
        self.start = to_timestamp(start)

        self.broker = BinanceBroker(
            api_key=api_key,
            api_secret=api_secret,
        )
        self.db = None
        self.candles = None

    @staticmethod
    def from_config(config_path: str, secrets_path: str):
        config = read_config(config_path)
        secrets = read_config(secrets_path)
        return TradingBot(**config, **secrets)

    def load_data(self, db_folder_path: str):
        self.db = CandleDB(folder_path=db_folder_path, broker_name=self.broker_name)

        optional_db_candles = self.db.get_candles(
            symbol=self.symbol, interval=self.interval, market=self.market
        )

        optional_new_candles = get_missing_historical_candles(
            symbol=self.symbol,
            interval=self.interval,
            market=self.market,
            broker=self.broker,
            latest_candle_time=get_latest_candle_timestamp(
                symbol=self.symbol,
                interval=self.interval,
                market=self.market,
                broker=self.broker,
                db_candles=optional_db_candles,
            ),
        )

        if optional_new_candles is not None:
            self.db.append_candles(
                df=optional_new_candles,
                symbol=self.symbol,
                interval=self.interval,
                market=self.market,
            )

        candles = merge_candle_dataframes(
            db_candles=optional_db_candles,
            new_candles=optional_new_candles,
        )

        candles = filter_binance_candle_dataframe(candles, includes=self.columns)
        self.candles = drop_rows_before(candles, start_time=self.start)

    def run(
        self,
        on_open: Callable[[websocket.WebSocketApp], None],
        on_close: Callable[[websocket.WebSocketApp], None],
        on_candle: Callable[[websocket.WebSocketApp, dict], None],
        on_candle_close: Callable[[websocket.WebSocketApp, pd.DataFrame], None],
    ):
        socket = websocket.WebSocketApp(
            url=f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}",
            on_open=on_open,
            on_close=on_close,
            on_message=append_binance_streaming_data(
                candle_df=self.candles,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )

        socket.run_forever()
