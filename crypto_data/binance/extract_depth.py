import numpy as np
from typing import Optional, List

import pandas as pd
from binance.client import Client

AGGREGATE_TRADE_ID = "aggregate_trade_id"
PRICE = "price"
QUANTITY = "quantity"
FIRST_TRADE_ID = "first_trade_id"
LAST_TRADE_ID = "last_trade_id"
TIMESTAMP = "timestamp"
IS_BUYER_MAKER = "is_buyer_maker"

COLUMNS = [
    AGGREGATE_TRADE_ID,
    PRICE,
    QUANTITY,
    FIRST_TRADE_ID,
    LAST_TRADE_ID,
    TIMESTAMP,
    IS_BUYER_MAKER,
]


def get_historical_depth():
    client = Client(
        api_key="0layjhc3VLp2Xw1ICEHlwZKL8Kgdi6jrj01KMQBPpvm2umDZdPdYSTeKnSnTujp2",
        api_secret="Tm8CJGWRHq0Sn3Pl31iBliWuwivfr7veINQbS7ebhZckwUUelb4Shle2ob2hZYzU",
    )

    f_agg_trades2 = client.futures_aggregate_trades(
        symbol="BTCUSDT", limit=100, startTime=1633065026000
    )
    pass


def get_aggregate_trades(
    client: Client, symbol: str, start_time: int, end_time: Optional[int] = None
):
    limit = 1000
    start_time = start_time * 1000

    agg_trades = client.futures_aggregate_trades(
        symbol=symbol, limit=limit, startTime=start_time
    )
    all_agg_trades: List[dict] = agg_trades
    while len(agg_trades) >= limit:
        last_agg_trade_id = agg_trades[-1]["a"]

        agg_trades = client.futures_aggregate_trades(
            symbol=symbol, limit=limit, fromId=last_agg_trade_id + 1
        )
        all_agg_trades.extend(agg_trades)

    agg_trade_df = pd.DataFrame(
        agg_trades,
        columns=COLUMNS,
    )

    agg_trade_df[PRICE] = agg_trade_df[PRICE].astype(dtype=np.int64)
    agg_trade_df[QUANTITY] = agg_trade_df[QUANTITY].astype(dtype=np.float)
    agg_trade_df[IS_BUYER_MAKER] = agg_trade_df[IS_BUYER_MAKER].astype(dtype=np.bool)


client = Client(
    api_key="0layjhc3VLp2Xw1ICEHlwZKL8Kgdi6jrj01KMQBPpvm2umDZdPdYSTeKnSnTujp2",
    api_secret="Tm8CJGWRHq0Sn3Pl31iBliWuwivfr7veINQbS7ebhZckwUUelb4Shle2ob2hZYzU",
)
get_aggregate_trades(client, "BTCUSDT", 1638939026)
