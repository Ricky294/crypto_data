import copy
import json
from typing import List, Callable

import numpy as np
import pandas as pd

from src.trading_bot.broker.binance_.schema import (
    OPEN_TIME,
    CLOSE_TIME,
    COLUMNS,
    COLUMN_DATA_TYPE_MAP,
    AGGREGATE_MAP,
    CHAR_COLUMN_MAP,
)
from src.trading_bot.utils import interval_ratio


def append_binance_streaming_data(
    candle_df: pd.DataFrame,
    on_candle: Callable,
    on_candle_close: Callable,
):
    def transform(socket, stream_data: str):
        new_candle: dict = json.loads(stream_data)["k"]
        on_candle(socket, new_candle)
        if new_candle["x"]:
            stream_df = transform_binance_stream_candle(new_candle, list(candle_df))
            candle_df = candle_df.append(stream_df, ignore_index=True)
            on_candle_close(socket, candle_df)

    return transform


def drop_rows_before(candle_df: pd.DataFrame, start_time: int):
    return candle_df[candle_df[OPEN_TIME] >= start_time]


def transform_binance_stream_candle(candle: dict, include_columns: List[str]):
    stream_df = transform_binance_stream_candle_to_dataframe(candle)
    stream_df = transform_binance_candle_dataframe_types(stream_df)
    return filter_binance_candle_dataframe(stream_df, include_columns)


def transform_binance_historical_candles(
    candles: List[List], include_columns: List[str]
):
    historical_df = transform_binance_historical_candles_to_dataframe(candles)
    historical_df = transform_binance_candle_dataframe_types(historical_df)
    return filter_binance_candle_dataframe(historical_df, include_columns)


def transform_binance_candle_dataframe_types(candles_df: pd.DataFrame):
    candles_df = candles_df.astype(
        {col: dtype for col, dtype in COLUMN_DATA_TYPE_MAP.items()}
    )

    candles_df[OPEN_TIME] = (candles_df[OPEN_TIME] / 1000).astype(np.uint32)
    candles_df[CLOSE_TIME] = (candles_df[CLOSE_TIME] / 1000).astype(np.uint32)
    return candles_df


def filter_binance_candle_dataframe(candles_df: pd.DataFrame, includes: List[str]):
    df_columns = list(candles_df)

    columns_to_drop = [column for column in df_columns if column not in includes]
    candles_df.drop(columns=columns_to_drop, inplace=True, axis=1)

    return candles_df


def transform_binance_historical_candles_to_dataframe(
    candles: List[List],
) -> pd.DataFrame:
    return pd.DataFrame(np.array(candles), columns=COLUMNS)


def transform_binance_stream_candle_to_dataframe(candle: dict):
    candle_copy = copy.deepcopy(candle)

    candle_copy.pop("x")  # is closed
    candle_copy.pop("s")  # symbol
    candle_copy.pop("i")  # interval
    candle_copy.pop("f")  # first trade id
    candle_copy.pop("L")  # last trade id

    stream_data_with_column_names = {
        CHAR_COLUMN_MAP[key]: [value] for key, value in candle_copy.items()
    }

    return pd.DataFrame(stream_data_with_column_names)


def aggregate_dataframe(
    candle_df: pd.DataFrame, candle_df_interval: str, agg_interval: str
):
    group_size = interval_ratio(agg_interval, candle_df_interval)

    candle_df_columns = list(candle_df)

    filtered_agg_info = {
        key: val for key, val in AGGREGATE_MAP.items() if key in candle_df_columns
    }

    agg_df = (
        candle_df.groupby(candle_df.index // group_size)
        .agg(filtered_agg_info)
        .reset_index(drop=True)
    )

    return agg_df
