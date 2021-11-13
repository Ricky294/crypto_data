from dataclasses import dataclass
from typing import Dict


class OrderError(Exception):
    def __init__(self, message):
        super().__init__(message)


@dataclass(frozen=True)
class Order:
    # Mandatory
    symbol: str
    side: str  # buy, sell

    # limit, market, stop, take_profit,
    # stop_market, take_profit_market, trailing_stop_market
    order_type: str

    # Optional
    position_side: str = None  # both, long, short
    time_in_force: str = None  # gtc, ioc, fok, gtx
    quantity: float = None
    reduce: bool = None
    close_position: bool = None
    price: int = None
    stop_price: int = None

    def check_order_validity_binance(self):
        if self.close_position and self.quantity is not None:
            raise OrderError("Quantity must be None if close_position is True!")
        elif self.order_type == "limit" and (
            self.time_in_force is None or self.quantity is None or self.price is None
        ):
            raise OrderError(
                "time_in_force, quantity and price is mandatory when creating limit order!"
            )
        elif self.order_type == "market" and self.quantity is None:
            raise OrderError("quantity is mandatory when creating limit orders!")
        elif self.order_type in ("stop", "take_profit") and (
            self.quantity is None or self.price is None or self.stop_price is None
        ):
            raise OrderError(
                "quantity, price and stop_price is mandatory when creating stop or take_profit order!"
            )
        elif (
            self.order_type in ("stop_market", "take_profit_market")
            and self.stop_price is None
        ):
            raise OrderError(
                "stop_price is mandatory when creating stop_market or take_profit_market orders!"
            )

    def as_binance_order(self) -> Dict:
        self.check_order_validity_binance()
        return {
            "symbol": self.symbol.upper(),
            "side": self.side.upper(),
            "type": self.order_type.upper(),
        }


o = Order(
    symbol="BTCUSDT",
    side="SELL",
    order_type="market",
    close_position=True,
    quantity=0.001,
)
o.as_binance_order()
