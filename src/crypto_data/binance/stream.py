from typing import Callable

import pandas as pd
from binance import ThreadedWebsocketManager
from binance.enums import FuturesType, ContractType

from src.crypto_data.binance.transform import append_binance_streaming_data


def candle_stream(
    symbol: str,
    interval: str,
    market: str,
    candles: pd.DataFrame,
    on_candle: Callable[[dict], None],
    on_candle_close: Callable[[pd.DataFrame], None],
):
    """
    Creates a binance data stream.
    """

    twm = ThreadedWebsocketManager()
    twm.start()

    if market.lower() == "spot":
        twm.start_kline_socket(
            symbol=symbol,
            interval=interval,
            callback=append_binance_streaming_data(
                candle_df=candles,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )

    elif market.lower() == "futures":
        twm.start_kline_futures_socket(
            symbol=symbol,
            interval=interval,
            callback=append_binance_streaming_data(
                candle_df=candles,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
            futures_type=FuturesType.USD_M,
            contract_type=ContractType.PERPETUAL,
        )
    else:
        raise ValueError("Market must be either 'futures' or 'spot'.")
