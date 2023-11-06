import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect


class QM_ST_003_VALID():
    """
    D-30 ~ D+7
    """

    def __init__(self, start=None, end=None):
        if start is None:
            raise Exception("")
        if utils.check_trading_day(start) is True:
            self.start = start
        else:
            raise Exception("거래일이 아닙니다")
        self.end = end
        self.search_end = utils.change_date(self.end, "days", 7)

        self.db = postgres_connect()

        ### StockCode ###
        query = f"""
            select stcd from stock_price
            where date='{self.start}'
            order by stcd
            """

        self.db.cursor.execute(query)
        self.stcds = tuple(map(lambda x: x[0], self.db.cursor.fetchall()))

        ### DataFrame ###
        query = f"""
            select date, stcd, open, high, low, close, values
            from stock_price
            where stcd in {self.stcds}
                and date between (select distinct date from stock_price
                        where date < '{self.start}'
                        order by date desc offset 230 limit 1)
                    and '{self.search_end}'
                and volume != 0
            order by stcd desc, date desc
            """

        self.db.cursor.execute(query)
        self.df = pd.DataFrame(self.db.cursor.fetchall())
        self.df = self.df[::-1].reset_index(drop=True)
        self.df.columns = ["date", "stcd",
                           "open", "high", "low", "close", "values"]

    def condition(self, df):
        try:
            search_list = []
            # 10 거래일 평균 거래대금
            mean_values_10 = df["values"].rolling(window=10).mean()
            # 전일 종가
            prev_close = df["close"].shift(1)
            # 이동평균선
            df["ma19"] = df["close"].rolling(window=19).mean()
            df["ma112"] = df["close"].rolling(window=112).mean()
            df["ma224"] = df["close"].rolling(window=224).mean()

            ### 조건 ###
            # 조건1: 10 거래일 평균 거래대금 대비 10배 상승
            df["cond1"] = mean_values_10.shift(1) * 10 < df["values"]
            # 조건2:
            df["cond2"] = (df["ma224"] < df["ma112"]) & (
                df["ma112"] < df["ma19"])

            dates = utils.date_range(self.start, self.end)

            for date in dates:
                if utils.check_trading_day(date):
                    aft_1d = utils.change_date(date, "days", 1)
                    aft_2d = utils.change_date(aft_1d, "days", 1)
                    aft_3d = utils.change_date(aft_2d, "days", 1)
                    aft_4d = utils.change_date(aft_3d, "days", 1)
                    aft_5d = utils.change_date(aft_4d, "days", 1)

                    if (
                        (df[df["date"] == date]["cond1"].values[0] is np.True_) and
                        (df[df["date"] == date]["cond2"].values[0] is np.True_)
                    ):
                        stcd = df[df["date"] == date]["stcd"].values[0]
                        buy_price = df[df["date"] == aft_1d]["open"].values[0]
                        aft_rate_1d = (df[df["date"] == aft_1d]["close"].values[0]) / buy_price - 1
                        aft_rate_2d = (df[df["date"] == aft_2d]["close"].values[0]) / buy_price - 1
                        aft_rate_3d = (df[df["date"] == aft_3d]["close"].values[0]) / buy_price - 1
                        aft_rate_4d = (df[df["date"] == aft_4d]["close"].values[0]) / buy_price - 1
                        aft_rate_5d = (df[df["date"] == aft_5d]["close"].values[0]) / buy_price - 1

                        value = (date, stcd, buy_price,
                                 aft_rate_1d, aft_rate_2d, aft_rate_3d, aft_rate_4d, aft_rate_5d)
                        search_list.append(value)
            return search_list
        
        except:
            return None

    def valid(self):
        for idx in range(len(self.stcds)):
            print(f"{idx + 1} / {len(self.stcds)}")

            tmp = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            values = self.condition(tmp)
            print(values)


class QM_ST_003_TRADE():

    def qm_st_003(self):
        print("qm_st_003")


class QM_ST_003_BACKTEST():
    def __init__(self):
        pass


class QM_ST_003_PRE_SEARCH():
    def __init__(self, date=None):
        self.date = date
