from typing import Optional, List

import pandas as pd
from binance import Client
from binance.enums import HistoricalKlinesType

from src.broker.binance_.trade import get_symbol_info
from src.broker.binance_.transform import transform_binance_historical_candles
from src.utils import round_down

SPOT_MARKET = "spot"
FUTURES_MARKET = "futures"


class BinanceBroker:
    def __init__(self, api_key: str, api_secret: str):
        self.client = Client(api_key=api_key, api_secret=api_secret)

    def get_historical_candle_dataframe(
        self,
        symbol: str,
        interval: str,
        market: str,
        include_columns: List[str],
        start_time: int,
        end_time: int = None,
        remove_last_open_candle: bool = True,
    ) -> Optional[pd.DataFrame]:
        market_map = {
            SPOT_MARKET: HistoricalKlinesType.SPOT,
            FUTURES_MARKET: HistoricalKlinesType.FUTURES,
        }

        candles = self.client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_time,
            end_str=end_time,
            klines_type=market_map[market],
        )
        # if list is not empty
        if candles:
            if remove_last_open_candle:
                candles.pop()

            return transform_binance_historical_candles(candles, include_columns)

    def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        position_side: str = None,
        quantity: float = None,
        price: float = None,
        stop_price: float = None,
        reduce_order: bool = None,
        close_position: bool = None,
    ):

        symbol_info = get_symbol_info(self.client, symbol)

        if price is not None:
            price = round_down(price, symbol_info.price_precision)
        if stop_price is not None:
            stop_price = round_down(stop_price, symbol_info.price_precision)

        if quantity is not None:
            quantity = round_down(quantity, symbol_info.quantity_precision)

        if position_side is not None:
            position_side = position_side.upper()
        if reduce_order is not None:
            reduce_order = str(reduce_order).lower()
        if close_position is not None:
            close_position = str(close_position).lower()

        self.client.futures_create_order(
            symbol=symbol.upper(),
            side=side.upper(),
            type=order_type.upper(),
            positionSide=position_side,
            quantity=quantity,
            reduceOnly=reduce_order,
            price=price,
            stopPrice=stop_price,
            closePosition=close_position,
        )
