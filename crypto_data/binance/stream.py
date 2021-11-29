from typing import Callable, Union, Dict

import pandas as pd
from binance.enums import FuturesType
from binance.streams import ThreadedWebsocketManager

from crypto_data.binance.transform import (
    append_binance_streaming_data,
    append_binance_multi_streaming_data,
)
from crypto_data.enum.market import Market
from crypto_data.binance.candle import StreamCandle


def candle_multi_stream(
    symbol_candles: Dict[str, pd.DataFrame],
    interval: str,
    market: Union[Market, str],
    on_candle: Callable[[StreamCandle], None],
    on_candle_close: Callable[[StreamCandle, Dict[str, pd.DataFrame]], None],
):
    """
    Creates a binance data stream.
    """

    twm = ThreadedWebsocketManager()
    twm.start()

    streams = [f"{symbol}@kline_{interval}" for symbol in symbol_candles.keys()]

    symbol_candles = {
        symbol.upper(): candles for symbol, candles in symbol_candles.items()
    }

    if str(market).upper() == "SPOT":
        twm.start_multiplex_socket(
            callback=append_binance_multi_streaming_data(
                symbol_candles=symbol_candles,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
            streams=streams,
        )

    elif str(market).upper() == "FUTURES":

        twm.start_futures_multiplex_socket(
            streams=streams,
            futures_type=FuturesType.USD_M,
            callback=append_binance_multi_streaming_data(
                symbol_candles=symbol_candles,
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )
    else:
        raise ValueError("Market must be either 'FUTURES' or 'SPOT'.")


def candle_stream(
    symbol: str,
    interval: str,
    market: Union[Market, str],
    candles: pd.DataFrame,
    on_candle: Callable[[StreamCandle], None],
    on_candle_close: Callable[[pd.DataFrame], None],
):
    """
    Creates a binance data stream.
    """

    twm = ThreadedWebsocketManager()
    twm.start()

    symbol = symbol.upper()
    if str(market).upper() == "SPOT":
        twm.start_kline_socket(
            symbol=symbol,
            interval=interval,
            callback=append_binance_streaming_data(
                candles=candles,
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
                on_candle=on_candle,
                on_candle_close=on_candle_close,
            ),
        )
    else:
        raise ValueError("Market must be either 'FUTURES' or 'SPOT'.")
