import json
from typing import List, Union

import numpy as np
import pandas as pd


class StreamCandle:
    def __init__(self, stream_data: Union[str, dict]):
        if isinstance(stream_data, str):
            stream_data = json.loads(stream_data)

        if "ps" in stream_data:
            self.symbol = stream_data["ps"]

        if "data" in stream_data:
            stream_data = stream_data["data"]

        candle = stream_data["k"]
        if "s" in candle:
            self.symbol = candle["s"]

        self.interval = candle["i"]
        self.open_time = int(candle["t"] / 1000)
        self.close_time = int(candle["T"] / 1000)
        self.open_price = float(candle["o"])
        self.close_price = float(candle["c"])
        self.high_price = float(candle["h"])
        self.low_price = float(candle["l"])
        self.volume = float(candle["v"])
        self.number_of_trades = int(candle["n"])
        self.closed = bool(candle["x"])
        self.quote_asset_volume = float(candle["q"])
        self.taker_buy_base_asset_volume = float(candle["V"])
        self.taker_buy_quote_asset_volume = float(candle["Q"])

    def to_dataframe(self, columns: List[str]):
        data = {key: [value] for key, value in self.__dict__.items() if key in columns}

        return pd.DataFrame(data)

    def to_numpy_array(self, columns: List[str]):
        values = [value for key, value in self.__dict__.items() if key in columns]

        return np.array(values)

    def __str__(self):
        return str(self.__dict__)
