import sqlite3
from typing import Tuple, List, Optional

import pandas as pd


class CandleDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.curs = self.conn.cursor()

    def append_candles(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str,
        market: str,
    ):
        df.to_sql(
            f"{symbol}_{interval}_{market}".lower(),
            self.conn,
            if_exists="append",
            index=False,
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

    def get_candles(
        self, symbol: str, interval: str, market: str
    ) -> Optional[pd.DataFrame]:
        table_name = f"{symbol}_{interval}_{market}".lower()
        if self._table_exists(table_name):
            return pd.read_sql(f"SELECT * FROM {table_name}", self.conn)

    def __del__(self):
        self.conn.commit()
        self.conn.close()
