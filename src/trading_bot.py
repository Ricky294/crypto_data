import pandas as pd
import websocket

from src.broker.binance_.broker import BinanceBroker
from src.broker.binance_.schema import OPEN_TIME
from src.broker.binance_.transform import append_binance_streaming_data
from src.db.candle_db import CandleDB
from src.utils import read_json, read_yaml, add_interval


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
    broker = BinanceBroker(name, **broker_secrets)

    db = CandleDB(name)
    db_candle_df = db.read_candle_df(symbol=symbol, interval=interval, market=market)

    start_time = (
        0
        if db_candle_df is None
        else add_interval(db_candle_df[OPEN_TIME].values[-1], interval) - 1
    )

    missing_candle_df = broker.get_historical_candle_dataframe(
        symbol=symbol,
        interval=interval,
        market=market,
        start_time=start_time,
        include_columns=columns,
    )

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
