import multiprocessing
import time

import pandas as pd
import websocket

from src.broker.binance_.broker import BinanceBroker
from src.broker.binance_.schema import OPEN_TIME, MARKET_MAP
from src.broker.binance_.transform import append_binance_streaming_data
from src.db.candle_db import CandleDB
from src.utils import read_json, read_yaml, progress_bar, interval_in_seconds

DOWNLOAD_LIMIT = 1000


def on_open(_):
    print("on open")


def on_close(_):
    print("on close")


def on_candle(_, data: dict):
    print(data)


def on_candle_close(_, candle_df: pd.DataFrame):
    print(candle_df)


def main():
    bot_config = read_yaml("configs", "trading_bot.yaml")
    name = bot_config["broker"]
    symbol = bot_config["symbol"]
    interval = bot_config["interval"]
    market = bot_config["market"]
    columns = bot_config["columns"]
    socket_url = bot_config["socket_url"]
    socket_type = bot_config["socket_type"]

    broker_secrets = read_json("secrets", f"{name}_secrets.json")
    broker = BinanceBroker(
        api_key=broker_secrets["api_key"], api_secret=broker_secrets["api_secret"]
    )

    db = CandleDB(name)
    db_candle_df = db.read_candle_df(symbol=symbol, interval=interval, market=market)

    start_time = (
        int(
            broker.client._get_earliest_valid_timestamp(
                symbol.upper(), interval, MARKET_MAP[market]
            )
            / 1000
        )
        if db_candle_df is None
        else int(db_candle_df[OPEN_TIME].values[-1]) + interval_in_seconds(interval)
    )

    progress_bar_thread = multiprocessing.Process(
        target=progress_bar,
        kwargs={
            "start_time": start_time,
            "end_time": int(time.time()),
            "interval": interval,
            "update_size": DOWNLOAD_LIMIT,
            "sleep_in_seconds": 1,
        },
    )
    progress_bar_thread.start()

    missing_candle_df = broker.get_historical_candle_dataframe(
        symbol=symbol,
        interval=interval,
        market=market,
        start_time=start_time * 1000,
        include_columns=columns,
        limit=DOWNLOAD_LIMIT,
    )

    progress_bar_thread.terminate()

    if db_candle_df is None and missing_candle_df is not None:
        candle_df = missing_candle_df
    elif db_candle_df is not None and missing_candle_df is None:
        candle_df = db_candle_df
    else:
        candle_df = db_candle_df.append(missing_candle_df)

    db.append_candle_df(candle_df, symbol=symbol, interval=interval, market=market)

    socket = websocket.WebSocketApp(
        url=f"{socket_url}{symbol}@{socket_type}_{interval}",
        on_open=on_open,
        on_close=on_close,
        on_message=append_binance_streaming_data(
            candle_df=candle_df, on_candle=on_candle, on_candle_close=on_candle_close
        ),
    )

    socket.run_forever()


if __name__ == "__main__":
    main()
