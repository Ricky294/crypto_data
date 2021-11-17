from typing import Callable

import pandas as pd
import websocket

from src.crypto_data.shared.transform import append_binance_streaming_data


def start_stream(
    symbol: str,
    interval: str,
    candles: pd.DataFrame,
    on_open: Callable[[websocket.WebSocketApp], None],
    on_close: Callable[[websocket.WebSocketApp], None],
    on_candle: Callable[[websocket.WebSocketApp, dict], None],
    on_candle_close: Callable[[websocket.WebSocketApp, pd.DataFrame], None],
):
    """
    Creates a binance data stream.
    """
    socket = websocket.WebSocketApp(
        url=f"wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}",
        on_open=on_open,
        on_close=on_close,
        on_message=append_binance_streaming_data(
            candle_df=candles,
            on_candle=on_candle,
            on_candle_close=on_candle_close,
        ),
    )

    socket.run_forever()
