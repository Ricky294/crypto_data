from typing import List, Optional

import pandas as pd

from crypto_data.shared.utils import exclude_values


def filter_dataframe_by_columns(
    candles_df: pd.DataFrame, all_columns, columns_to_include: List[str]
):
    columns_to_drop = exclude_values(
        values=all_columns, include_values=columns_to_include
    )
    candles_df.drop(columns=columns_to_drop, inplace=True, axis=1)
    return candles_df


def safe_merge_dataframes(
    append_to_df: Optional[pd.DataFrame],
    other_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    if append_to_df is None and other_df is None:
        raise ValueError("At least one of the dataframes must not be None.")
    if other_df is None:
        return append_to_df
    if append_to_df is None:
        return other_df

    return append_to_df.append(other_df, ignore_index=True)


def aggregate_dataframe(df: pd.DataFrame, aggregate_info: dict, group_size: int):
    return df.groupby(df.index // group_size).agg(aggregate_info).reset_index(drop=True)
