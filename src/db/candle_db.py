import sqlite3
from typing import Tuple, List, Optional

import pandas as pd


class CandleDB:
    def __init__(self, broker: str):
        self.db_name = f"{broker}_candle.db"
        self.conn = sqlite3.connect(self.db_name)
        self.curs = self.conn.cursor()

    def append_candle_df(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str,
        market: str,
    ):
        df.to_sql(
            f"{symbol}_{interval}_{market}", self.conn, if_exists="append", index=False
        )

    def _table_exists(self, table_name: str):

        self.curs.execute(
            """SELECT name FROM sqlite_schema
            WHERE type='table'
            ORDER BY name;"""
        )

        self.conn.commit()
        tables: List[Tuple] = self.curs.fetchall()

        for table in tables:
            if table_name in table:
                return True

        return False

    def read_candle_df(
        self, symbol: str, interval: str, market: str
    ) -> Optional[pd.DataFrame]:
        table_name = f"{symbol}_{interval}_{market}"
        if self._table_exists(table_name):
            return pd.read_sql(f"SELECT * FROM {table_name}", self.conn)

    def read_first_row(self):
        return pd.read_sql(
            f"SELECT * FROM table ORDER BY column DESC LIMIT 1", self.conn
        )

    def __del__(self):
        self.conn.commit()
        self.conn.close()
