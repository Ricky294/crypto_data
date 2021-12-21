from typing import List, Callable

import numpy as np

from crypto_data.binance.candle import StreamCandle


def append_binance_streaming_data(
    candles: np.ndarray,
    columns: List[str],
    on_candle: Callable[[StreamCandle], any],
    on_candle_close: Callable[[np.ndarray], any],
):
    def transform(stream_data: dict):
        nonlocal candles
        nonlocal on_candle
        nonlocal on_candle_close

        candle = StreamCandle(stream_data)
        on_candle(candle)

        if candle.closed:
            stream_candle = candle.to_numpy_array(columns=columns).reshape(
                1, len(columns)
            )
            candles = np.concatenate((candles, stream_candle), axis=0)

            on_candle_close(candles)

    return transform
