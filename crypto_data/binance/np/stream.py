from typing import Callable, Union, List

import numpy as np
from binance.enums import FuturesType
from binance.streams import ThreadedWebsocketManager

from crypto_data.binance.np.transform import (
    append_binance_streaming_data,
)
from crypto_data.enum.market import Market
from crypto_data.binance.candle import StreamCandle


def candle_stream(
    symbol: str,
    interval: str,
    market: Union[Market, str],
    columns: List[str],
    candles: np.ndarray,
    on_candle: Callable[[StreamCandle], None],
    on_candle_close: Callable[[np.ndarray], None],
):
    """
    Creates a binance data stream.
    """

    print(
        f"Starting candle stream on (symbol: {symbol}, market: {market}, interval: {interval})..."
    )
    twm = ThreadedWebsocketManager()
    twm.start()

    symbol = symbol.upper()
    if str(market).upper() == "SPOT":
        twm.start_kline_socket(
            symbol=symbol,
            interval=interval,
            callback=append_binance_streaming_data(
                candles=candles,
                columns=columns,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )

    elif str(market).upper() == "FUTURES":
        twm.start_kline_futures_socket(
            symbol=symbol,
            interval=interval,
            futures_type=FuturesType.USD_M,
            callback=append_binance_streaming_data(
                candles=candles,
                columns=columns,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )
    else:
        raise ValueError("Market must be either 'FUTURES' or 'SPOT'.")

    print(f"Candle stream started.")
