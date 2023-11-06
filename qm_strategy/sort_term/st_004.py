import math
import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect


class QM_ST_004_VALID():
    """
    """
    def __init__(self):
        self.db = postgres_connect()

        query = f"""
            select distinct date from cache_soaring_value
            where date < '20230701'
            order by date
            """
        self.db.cursor.execute(query)
        self.dates = tuple(map(lambda x: x[0], self.db.cursor.fetchall()))
        for i in range(len(self.dates) - 1):
            query = f"""
                select date, stcd, rate, open, close from stock_price
                where stcd in (
                        select stcd from cache_soaring_value
                        where date = '{self.dates[i]}'
                        order by value desc
                        limit 5
                    )
                    and date between '{self.dates[i]}' and '{self.dates[i+1]}'
                order by stcd, date
                """
            self.db.cursor.execute(query)
            self.df = pd.DataFrame(self.db.cursor.fetchall())
            self.df.columns = ["date", "stcd", "rate", "open", "close"]

            self.df["cond1"] = [True if 0 < r < 20 else False for r in self.df["rate"].shift(1)]
            self.df["cond2"] = [True if date == self.dates[i + 1] else False for date in self.df["date"]]
            self.df["diff"] = self.df["close"] - self.df["open"]
            self.df["diff_rate"] = [100 * d / o if c1 == True and c2 == True else None for [c1, c2, d, o] in self.df[["cond1", "cond2", "diff", "open"]].values]
            _mean = self.df["diff_rate"].mean()
            _cnt = self.df["diff_rate"].count()
            print(self.df)
            # print(self.dates[i], round(_mean, 2), _cnt)

QM_ST_004_VALID()