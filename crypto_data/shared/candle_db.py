import sqlite3
from pandas.io.sql import DatabaseError
from typing import Optional

import pandas as pd


class CandleDB:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def append_candles(
        self,
        df: pd.DataFrame,
        table_name: str,
    ):
        conn = sqlite3.connect(self.db_path)
        df.to_sql(
            table_name,
            conn,
            if_exists="append",
            index=False,
        )
        conn.close()

    def get_candles(self, table_name) -> Optional[pd.DataFrame]:
        conn = sqlite3.connect(self.db_path)
        try:
            query_result = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        except DatabaseError:
            query_result = None
        conn.close()
        return query_result
