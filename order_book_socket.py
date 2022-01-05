import json

import pandas as pd
import websocket


class AggregateTrade:

    __slots__ = (
        "event_time",
        "trade_time",
        "symbol",
        "aggregate_trade_id",
        "maker_or_taker",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
    )

    def __init__(self, agg_trade: dict):
        self.event_time = agg_trade["E"]
        self.trade_time = agg_trade["T"]
        self.symbol = agg_trade["s"]
        self.aggregate_trade_id = int(agg_trade["a"])
        self.maker_or_taker = "maker" if bool(agg_trade["m"]) else "taker"
        self.price = float(agg_trade["p"])
        self.quantity = float(agg_trade["q"])
        self.first_trade_id = int(agg_trade["f"])
        self.last_trade_id = int(agg_trade["l"])

    def __str__(self):
        return f"""Aggregate trade (
event time: {self.event_time}, trade_time: {self.trade_time}, symbol: {self.symbol} ,
buyer: {self.maker_or_taker}, price: {self.price}, quantity: {self.quantity}
"""


def cb(agg_trade_data):
    print(agg_trade_data)
    """ask_df, bid_df = tuple(
        pd.DataFrame(data=agg_trade_data[side], columns=["price", "quantity"], dtype=float)
        for side in ['a', 'b']
    )
    ask_df["side"] = "ask"
    bid_df["side"] = "bid"

    ask_bid_df = pd.concat((ask_df, bid_df), axis="index", ignore_index=True, sort=True)

    fig, ax = plt.subplots()

    sns.ecdfplot(x="price", weights="quantity", stat="count", complementary=True, data=bid_df, ax=ax)
    sns.ecdfplot(x="price", weights="quantity", stat="count", data=ask_df, ax=ax)
    sns.scatterplot(x="price", y="quantity", hue="side", data=ask_bid_df, ax=ax)

    ax.set_xlabel("Price")
    ax.set_ylabel("Quantity")

    plt.show()
    pass"""


def on_message(ws, message):
    data = json.loads(message)
    depth_data = data["data"]

    ask_df, bid_df = tuple(
        pd.DataFrame(data=depth_data[side], columns=["price", "quantity"], dtype=float)
        for side in ["a", "b"]
    )
    ask_df["side"] = "ask"
    bid_df["side"] = "bid"

    ask_bid_df = pd.concat((ask_df, bid_df), axis="index", ignore_index=True, sort=True)


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    print("open")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://fstream.binance.com/stream?streams=btcusdt@depth",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever()
