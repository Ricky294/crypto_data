from typing import Dict, List, Any
from binance import Client

# Example:
# {
# 'symbol': 'BTCUSDT', 'pair': 'BTCUSDT', 'contractType': 'PERPETUAL', 'deliveryDate': 4133404800000,
# 'onboardDate': 1569398400000, 'status': 'TRADING', 'maintMarginPercent': '2.5000', 'requiredMarginPercent': '5.0000',
# 'baseAsset': 'BTC', 'quoteAsset': 'USDT', 'marginAsset': 'USDT', 'pricePrecision': 2, 'quantityPrecision': 3,
# 'baseAssetPrecision': 8, 'quotePrecision': 8, 'underlyingType': 'COIN', 'underlyingSubType': [], 'settlePlan': 0,
# 'triggerProtect': '0.0500', 'liquidationFee': '0.030000', 'marketTakeBound': '0.05',
# 'filters': [
#     {'minPrice': '556.72', 'maxPrice': '4529764', 'filterType': 'PRICE_FILTER', 'tickSize': '0.01'},
#     {'stepSize': '0.001', 'filterType': 'LOT_SIZE', 'maxQty': '1000', 'minQty': '0.001'},
#     {'stepSize': '0.001', 'filterType': 'MARKET_LOT_SIZE', 'maxQty': '300', 'minQty': '0.001'},
#     {'limit': 200, 'filterType': 'MAX_NUM_ORDERS'},
#     {'limit': 10, 'filterType': 'MAX_NUM_ALGO_ORDERS'},
#     {'notional': '5', 'filterType': 'MIN_NOTIONAL'},
#     {'multiplierDown': '0.9500', 'multiplierUp': '1.0500', 'multiplierDecimal': '4', 'filterType': 'PERCENT_PRICE'}
# ],
# 'orderTypes': ['LIMIT', 'MARKET', 'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET'],
# 'timeInForce': ['GTC', 'IOC', 'FOK', 'GTX']
# }


class SymbolInfo:
    def __init__(self, symbol_info: Dict[str, Any]):
        self.symbol: str = symbol_info["symbol"]
        self.base_asset: str = symbol_info["baseAsset"]
        self.quote_asset: str = symbol_info["quoteAsset"]
        self.margin_asset: str = symbol_info["marginAsset"]
        self.price_precision: int = symbol_info["pricePrecision"]
        self.quantity_precision: int = symbol_info["quantityPrecision"]
        self.base_asset_precision: int = symbol_info["baseAssetPrecision"]
        self.quote_precision: int = symbol_info["quotePrecision"]
        self.underlying_type: str = symbol_info["underlyingType"]
        self.filters: List[Dict] = symbol_info["filters"]
        self.order_types: List[str] = symbol_info["orderTypes"]
        self.time_in_force: List[str] = symbol_info["timeInForce"]

    def get_filter(self, filter_type: str):
        for filter_ in self.filters:
            if filter_type == filter_["filterType"]:
                return filter_


def get_symbol_info(client: Client, symbol: str) -> SymbolInfo:
    exchange_info = client.futures_exchange_info()
    for symbol_info in exchange_info["symbols"]:
        if symbol_info["symbol"] == symbol:
            return SymbolInfo(symbol_info)
