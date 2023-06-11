import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect


class QM_ST_002():

    _screen_real_stock = "2000"

    ### 전략 실행 함수 ###
    def qm_st_002(self):
        print("qm_st_002")


class QM_ST_002_PER_SEARCH():
    def __init__(self, date=None):
        self.date = date
        if date is None:
            self.date = utils._today
        if utils.check_trading_day(self.date) is False:
            raise Exception("입력일이 거래일이 아닙니다.")

        self.db = postgres_connect()

        ### StockCode ###
        query = f"""
            select stcd from stock_price
            where date = '{self.date}'
            order by stcd
            """

        self.db.cursor.execute(query)
        self.stcds = tuple(map(lambda x: x[0], self.db.cursor.fetchall()))
        self.denom = len(self.stcds) // 10

        ### DataFrame ###
        query = f"""
            select date, stcd, rate, open, high, low, close, values
            from stock_price
            where stcd in {self.stcds}
                and date between (select distinct date from stock_price
                        where date < '{self.date}'
                        order by date desc offset 239 limit 1)
                    and '{self.date}'
                and volume != 0
            order by stcd desc, date desc
            """

        self.db.cursor.execute(query)
        self.df = pd.DataFrame(self.db.cursor.fetchall())[
            ::-1].reset_index(drop=True)
        self.df.columns = ["date", "stcd", "rate", "open",
                           "high", "low", "close", "values"]

    def condition(self, df):
        try:
            # 10 거래일 평균 거래대금
            mean_values_10 = df["values"].rolling(window=10).mean()
            # 전일 종가
            prev_close = df["close"].shift(1)
            # 이동평균선
            df["ma112"] = df["close"].rolling(window=112).mean()
            df["ma224"] = df["close"].rolling(window=224).mean()
            # 조건1: 10 거래일 평균 거래대금 대비 10배 상승
            df["cond1"] = mean_values_10.shift(1) * 10 < df["values"]
            # 조건2: 최근 40일 조건1 을 충족한 적이 없어야함
            df["cond2"] = df["cond1"].shift(1).rolling(window=40).sum() == 0
            # 조건3: 이평선 밴드 폭이 좁아야 함(10 %)
            df["cond3"] = abs(df["ma112"] - df["ma224"]) * \
                2 / (df["ma112"] + df["ma224"]) < 0.1
            # 조건4: 5% 이상 양봉
            df["cond4"] = df["rate"] >= 5
            # 조건5: 해당일 224일선 돌파
            df["cond5"] = (df["ma224"] < df["close"]) & (
                (df["ma224"] > df["low"]) | (df["ma224"] > prev_close))
            ###
            if (
                (df[df["date"] == self.date]["cond1"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond2"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond3"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond4"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond5"].values[0] is np.True_)
            ):
                stcd = df[df["date"] == self.date]["stcd"].values[0]
                value = (self.date, stcd)

                return value

        except:
            return None
        
    def search(self):
        self.saerch_list = []
        for idx in range(len(self.stcds)):
            if idx % self.denom == 0 and idx // self.denom <= 10:
                print(f"{idx // self.denom} / 10")

            tmp = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            value = self.condition(tmp)

            if value is not None:
                self.saerch_list.append(value)
                print(value)


class QM_ST_002_VALID():
    """
    date: 최근 거래일 - 9
    """

    def __init__(self, date=None):
        if date is None:
            raise Exception("최대 입력 값은 최근 거래일-4 일 입니다")
        if utils.check_trading_day(date) is True:
            self.date = date
        else:
            raise Exception("거래일이 아닙니다.")

        self.db = postgres_connect()

        ### StockCode ###
        query = f"""
            select stcd from stock_price
            where date = '{self.date}'
            order by stcd
            """

        self.db.cursor.execute(query)
        self.stcds = tuple(map(lambda x: x[0], self.db.cursor.fetchall()))
        self.denom = len(self.stcds) // 10

        ### 확인 할 날짜 ###
        self.aft_1d = utils.change_date(self.date, "days", 1)
        self.aft_2d = utils.change_date(self.aft_1d, "days", 1)
        self.aft_3d = utils.change_date(self.aft_2d, "days", 1)
        self.aft_4d = utils.change_date(self.aft_3d, "days", 1)
        self.aft_5d = utils.change_date(self.aft_4d, "days", 1)
        self.aft_6d = utils.change_date(self.aft_5d, "days", 1)
        self.aft_7d = utils.change_date(self.aft_6d, "days", 1)
        self.aft_8d = utils.change_date(self.aft_7d, "days", 1)
        self.aft_9d = utils.change_date(self.aft_8d, "days", 1)

        print(self.date, self.aft_1d, self.aft_2d,
              self.aft_3d, self.aft_4d, self.aft_5d)

        ### DataFrame ###
        query = f"""
            select date, stcd, rate, open, high, low, close, values
            from stock_price
            where stcd in {self.stcds}
                and date between (select distinct date from stock_price
                        where date < '{self.date}'
                        order by date desc offset 239 limit 1)
                    and '{self.aft_9d}'
                and volume != 0
            order by stcd desc, date desc
            """

        self.db.cursor.execute(query)
        self.df = pd.DataFrame(self.db.cursor.fetchall())[
            ::-1].reset_index(drop=True)
        self.df.columns = ["date", "stcd", "rate", "open",
                           "high", "low", "close", "values"]

    def condition(self, df):
        try:
            # 10 거래일 평균 거래대금
            mean_values_10 = df["values"].rolling(window=10).mean()
            # 전일 종가
            prev_close = df["close"].shift(1)
            # 이동평균선
            df["ma112"] = df["close"].rolling(window=112).mean()
            df["ma224"] = df["close"].rolling(window=224).mean()
            # 조건1: 10 거래일 평균 거래대금 대비 10배 상승
            df["cond1"] = mean_values_10.shift(1) * 10 < df["values"]
            # 조건2: 최근 40일 조건1 을 충족한 적이 없어야함
            df["cond2"] = df["cond1"].shift(1).rolling(window=40).sum() == 0
            # 조건3: 이평선 밴드 폭이 좁아야 함(10 %)
            df["cond3"] = abs(df["ma112"] - df["ma224"]) * \
                2 / (df["ma112"] + df["ma224"]) < 0.1
            # 조건4: 5% 이상 양봉
            df["cond4"] = df["rate"] >= 5
            # 조건5: 해당일 224일선 돌파
            df["cond5"] = (df["ma224"] < df["close"]) & (
                (df["ma224"] > df["low"]) | (df["ma224"] > prev_close))
            ###
            if (
                (df[df["date"] == self.date]["cond1"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond2"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond3"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond4"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond5"].values[0] is np.True_)
            ):
                stcd = df[df["date"] == self.date]["stcd"].values[0]
                buy_price = df[df["date"] == self.aft_1d]["open"].values[0]
                af_1d = (df[df["date"] == self.aft_1d]
                         ["close"].values[0]) / buy_price - 1
                af_2d = (df[df["date"] == self.aft_2d]
                         ["close"].values[0]) / buy_price - 1
                af_3d = (df[df["date"] == self.aft_3d]
                         ["close"].values[0]) / buy_price - 1
                af_4d = (df[df["date"] == self.aft_4d]
                         ["close"].values[0]) / buy_price - 1
                af_5d = (df[df["date"] == self.aft_5d]
                         ["close"].values[0]) / buy_price - 1
                af_6d = (df[df["date"] == self.aft_6d]
                         ["close"].values[0]) / buy_price - 1
                af_7d = (df[df["date"] == self.aft_7d]
                         ["close"].values[0]) / buy_price - 1
                af_8d = (df[df["date"] == self.aft_8d]
                         ["close"].values[0]) / buy_price - 1
                af_9d = (df[df["date"] == self.aft_9d]
                         ["close"].values[0]) / buy_price - 1
                value = (self.date, stcd, buy_price,
                         af_1d, af_2d, af_3d, af_4d, af_5d,
                         af_6d, af_7d, af_8d, af_9d)

                return value

        except:
            return None

    def valid(self):
        self.search_list = []
        for idx in range(len(self.stcds)):
            if idx % self.denom == 0 and idx // self.denom <= 10:
                print(f"{idx // self.denom} / 10")

            tmp = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            value = self.condition(tmp)

            if value is not None:
                self.search_list.append(value)
                print(value)

        if len(self.search_list) != 0:
            self.db.multiInsertDB("valid_st_002", self.search_list)
