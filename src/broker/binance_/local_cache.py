import pandas as pd


def create_engine(host: str, port: int, db_name: str):
    return create_engine(f"postgresql://dbuser:dbpass@{host}:{port}/{db_name}")


# pd.DataFrame (store symbol, and interval as metadata)
def store_dataframe(symbol: str, interval: str, engine, new_df: pd.DataFrame):
    new_df.to_sql(f"{symbol}_{interval}", engine, if_exists="append")
