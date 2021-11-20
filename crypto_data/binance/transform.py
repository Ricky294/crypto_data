from typing import List, Callable

import numpy as np
import pandas as pd

from crypto_data.shared.transform import (
    filter_dataframe_by_columns,
    aggregate_dataframe,
)
from crypto_data.binance.schema import (
    OPEN_TIME,
    CLOSE_TIME,
    COLUMNS,
    COLUMN_DATA_TYPE_MAP,
    AGGREGATE_MAP,
    CHAR_COLUMN_MAP,
)
from crypto_data.shared.utils import interval_ratio


def append_binance_streaming_data(
    candle_df: pd.DataFrame,
    on_candle: Callable,
    on_candle_close: Callable,
):
    def transform(stream_data: dict):
        nonlocal candle_df
        nonlocal on_candle
        nonlocal on_candle_close

        new_candle: dict = stream_data["k"]
        on_candle(new_candle)
        if new_candle["x"]:
            stream_df = transform_binance_stream_candle(new_candle, list(candle_df))
            candle_df = candle_df.append(stream_df, ignore_index=True)
            on_candle_close(candle_df)

    return transform


def transform_binance_stream_candle(candle: dict, columns_to_include: List[str]):
    stream_df = transform_binance_stream_candle_to_dataframe(candle)
    stream_df = transform_binance_candle_dataframe_types(stream_df)
    return filter_dataframe_by_columns(stream_df, COLUMNS, columns_to_include)


def transform_binance_historical_candles(
    candles: List[List], columns_to_include: List[str]
):
    historical_df = pd.DataFrame(np.array(candles), columns=COLUMNS)
    historical_df = transform_binance_candle_dataframe_types(historical_df)
    return filter_dataframe_by_columns(historical_df, COLUMNS, columns_to_include)


def transform_binance_candle_dataframe_types(candles_df: pd.DataFrame):
    candles_df = candles_df.astype(
        {col: dtype for col, dtype in COLUMN_DATA_TYPE_MAP.items()}
    )

    candles_df[OPEN_TIME] = (candles_df[OPEN_TIME] / 1000).astype(np.uint32)
    candles_df[CLOSE_TIME] = (candles_df[CLOSE_TIME] / 1000).astype(np.uint32)
    return candles_df


def transform_binance_stream_candle_to_dataframe(candle: dict):
    stream_data_with_column_names = {
        CHAR_COLUMN_MAP[key]: [value]
        for key, value in candle.items()
        if key in CHAR_COLUMN_MAP
    }

    return pd.DataFrame(stream_data_with_column_names)


def aggregate_candle_dataframe(
    candle_df: pd.DataFrame, candle_df_interval: str, agg_interval: str
):
    group_size = interval_ratio(agg_interval, candle_df_interval)

    candle_df_columns = list(candle_df)

    filtered_agg_info = {
        key: val for key, val in AGGREGATE_MAP.items() if key in candle_df_columns
    }

    return aggregate_dataframe(candle_df, filtered_agg_info, group_size)
