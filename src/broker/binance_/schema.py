from binance.enums import HistoricalKlinesType

OPEN_TIME = "open_time"
OPEN_PRICE = "open_price"
HIGH_PRICE = "high_price"
LOW_PRICE = "low_price"
CLOSE_PRICE = "close_price"
VOLUME = "volume"
CLOSE_TIME = "close_time"
QUOTE_ASSET_VOLUME = "quote_asset_volume"
NUMBER_OF_TRADES = "number_of_trades"
TAKER_BUY_BASE_ASSET_VOLUME = "taker_buy_base_asset_volume"
TAKER_BUY_QUOTE_ASSET_VOLUME = "taker_buy_quote_asset_volume"
IGNORE = "ignore"

COLUMNS = [
    OPEN_TIME,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
    VOLUME,
    CLOSE_TIME,
    QUOTE_ASSET_VOLUME,
    NUMBER_OF_TRADES,
    TAKER_BUY_BASE_ASSET_VOLUME,
    TAKER_BUY_QUOTE_ASSET_VOLUME,
    IGNORE,
]

CHAR_MAP = "tohlcvTqnVQi"

CHAR_COLUMN_MAP = {
    "t": OPEN_TIME,
    "o": OPEN_PRICE,
    "h": HIGH_PRICE,
    "l": LOW_PRICE,
    "c": CLOSE_PRICE,
    "v": VOLUME,
    "T": CLOSE_TIME,
    "q": QUOTE_ASSET_VOLUME,
    "n": NUMBER_OF_TRADES,
    "V": TAKER_BUY_BASE_ASSET_VOLUME,
    "Q": TAKER_BUY_QUOTE_ASSET_VOLUME,
    "B": IGNORE,
}

COLUMN_CHAR_MAP = {
    OPEN_TIME: "t",
    OPEN_PRICE: "o",
    HIGH_PRICE: "h",
    LOW_PRICE: "l",
    CLOSE_PRICE: "c",
    VOLUME: "v",
    CLOSE_TIME: "T",
    QUOTE_ASSET_VOLUME: "q",
    NUMBER_OF_TRADES: "n",
    TAKER_BUY_BASE_ASSET_VOLUME: "V",
    TAKER_BUY_QUOTE_ASSET_VOLUME: "Q",
    IGNORE: "i",
}

COLUMN_DATA_TYPE_MAP = {
    OPEN_TIME: "uint64",
    OPEN_PRICE: "float64",
    HIGH_PRICE: "float64",
    LOW_PRICE: "float64",
    CLOSE_PRICE: "float64",
    VOLUME: "float64",
    CLOSE_TIME: "uint64",
    QUOTE_ASSET_VOLUME: "float64",
    NUMBER_OF_TRADES: "uint32",
    TAKER_BUY_BASE_ASSET_VOLUME: "float64",
    TAKER_BUY_QUOTE_ASSET_VOLUME: "float64",
    IGNORE: "float64",
}

AGGREGATE_MAP = {
    OPEN_TIME: "first",
    OPEN_PRICE: "first",
    HIGH_PRICE: "max",
    LOW_PRICE: "min",
    CLOSE_PRICE: "last",
    VOLUME: "sum",
    CLOSE_TIME: "last",
    QUOTE_ASSET_VOLUME: "sum",
    NUMBER_OF_TRADES: "sum",
    TAKER_BUY_BASE_ASSET_VOLUME: "sum",
    TAKER_BUY_QUOTE_ASSET_VOLUME: "sum",
    IGNORE: "sum",
}


SPOT_MARKET = "spot"
FUTURES_MARKET = "futures"

MARKET_MAP = {
    SPOT_MARKET: HistoricalKlinesType.SPOT,
    FUTURES_MARKET: HistoricalKlinesType.FUTURES,
}
