import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect


class QM_ST_002_PRE_SEARCH():
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
            df["ma19"] = df["close"].rolling(window=19).mean()
            df["ma112"] = df["close"].rolling(window=112).mean()
            df["ma224"] = df["close"].rolling(window=224).mean()
            df["sup1"] = df[["ma19", "ma112", "ma224"]].min(axis=1)
            # 조건1: 10 거래일 평균 거래대금 대비 10배 상승
            df["cond1"] = mean_values_10.shift(1) * 10 < df["values"]
            # 조건2: 최근 40일 조건1 을 충족한 적이 없어야함
            df["cond2"] = df["cond1"].shift(1).rolling(window=40).sum() == 0
            # 조건3: 이평선 밴드 폭이 좁아야 함(10 %)
            df["cond3"] = abs(df["ma112"] - df["ma224"]) * \
                2 / (df["ma112"] + df["ma224"]) < 0.1
            # 조건4: 5% 이상 양봉
            df["cond4"] = (df["rate"] >= 5) & (df["open"] < df["close"])
            # 조건5: 해당일 224일선 돌파
            df["cond5"] = (df["sup1"] < df["close"]) & (
                (df["sup1"] > df["low"]) | (df["sup1"] > prev_close))
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
        self.search_list = []
        for idx in range(len(self.stcds)):
            if idx % self.denom == 0 and idx // self.denom <= 10:
                print(f"{idx // self.denom} / 10")

            tmp = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            value = self.condition(tmp)

            if value is not None:
                self.search_list.append(value)
                print(value)


class QM_ST_002_VALID():
    """
    date: 최근 거래일 - 9
    """

    def __init__(self, start=None, end=None):
        if start is None:
            raise Exception("최대 입력 값은 최근 거래일-4 일 입니다")
        if utils.check_trading_day(start) is True:
            self.start = start
        else:
            raise Exception("거래일이 아닙니다.")
        self.end = end
        self.search_end = utils.change_date(self.end, "days", 9)

        self.db = postgres_connect()

        ### StockCode ###
        query = f"""
            select stcd from stock_price
            where date = '{self.start}'
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
                        order by date desc offset 239 limit 1)
                    and '{self.search_end}'
                and volume != 0
            order by stcd desc, date desc
            """

        self.db.cursor.execute(query)
        self.df = pd.DataFrame(self.db.cursor.fetchall())[
            ::-1].reset_index(drop=True)
        self.df.columns = ["date", "stcd",
                           "open", "high", "low", "close", "values"]

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
            df["cond4"] = (df["close"] / prev_close >= 1.05) &\
                (df["open"] < df["close"])
            # 조건5: 해당일 224일선 돌파
            df["cond5"] = (df["ma224"] < df["close"]) & (
                (df["ma224"] > df["low"]) | (df["ma224"] > prev_close))

            dates = utils.date_range(self.start, self.end)

            search_list = []
            for date in dates:
                if utils.check_trading_day(date) is True:
                    ### 확인 할 날짜 ###
                    aft_1d = utils.change_date(date, "days", 1)
                    aft_2d = utils.change_date(aft_1d, "days", 1)
                    aft_3d = utils.change_date(aft_2d, "days", 1)
                    aft_4d = utils.change_date(aft_3d, "days", 1)
                    aft_5d = utils.change_date(aft_4d, "days", 1)
                    aft_6d = utils.change_date(aft_5d, "days", 1)
                    aft_7d = utils.change_date(aft_6d, "days", 1)
                    aft_8d = utils.change_date(aft_7d, "days", 1)
                    aft_9d = utils.change_date(aft_8d, "days", 1)

                    if (
                        (df[df["date"] == date]["cond1"].values[0] is np.True_) and
                        (df[df["date"] == date]["cond2"].values[0] is np.True_) and
                        (df[df["date"] == date]["cond3"].values[0] is np.True_) and
                        (df[df["date"] == date]["cond4"].values[0] is np.True_) and
                        (df[df["date"] == date]["cond5"].values[0] is np.True_)
                    ):
                        stcd = df[df["date"] == date]["stcd"].values[0]
                        buy_price = df[df["date"] == aft_1d]["open"].values[0]
                        af_1d = (df[df["date"] == aft_1d]
                                 ["close"].values[0]) / buy_price - 1
                        af_2d = (df[df["date"] == aft_2d]
                                 ["close"].values[0]) / buy_price - 1
                        af_3d = (df[df["date"] == aft_3d]
                                 ["close"].values[0]) / buy_price - 1
                        af_4d = (df[df["date"] == aft_4d]
                                 ["close"].values[0]) / buy_price - 1
                        af_5d = (df[df["date"] == aft_5d]
                                 ["close"].values[0]) / buy_price - 1
                        af_6d = (df[df["date"] == aft_6d]
                                 ["close"].values[0]) / buy_price - 1
                        af_7d = (df[df["date"] == aft_7d]
                                 ["close"].values[0]) / buy_price - 1
                        af_8d = (df[df["date"] == aft_8d]
                                 ["close"].values[0]) / buy_price - 1
                        af_9d = (df[df["date"] == aft_9d]
                                 ["close"].values[0]) / buy_price - 1
                        value = (date, stcd, buy_price,
                                 af_1d, af_2d, af_3d, af_4d, af_5d,
                                 af_6d, af_7d, af_8d, af_9d)
                        search_list.append(value)
            return search_list

        except:
            return None

    def valid(self):

        for idx in range(len(self.stcds)):
            print(f"{idx + 1} / {len(self.stcds)}")

            tmp = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            values = self.condition(tmp)

            if len(values) != 0:
                print(f"{self.stcds[idx]} 저장")
                self.db.multiInsertDB("valid_st_002", values)


class QM_ST_002_BACKTEST():

    def __init__(self, date):
        self.target_rate = 0.03
        if utils.check_trading_day(date) is True:
            self.date = date
            self.aft_1d = utils.change_date(self.date, "days", 1)
            self.aft_2d = utils.change_date(self.aft_1d, "days", 1)
            self.aft_3d = utils.change_date(self.aft_2d, "days", 1)
            self.aft_4d = utils.change_date(self.aft_3d, "days", 1)

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

        query = f"""
            select date, stcd, open, high, low, close, values
            from stock_price
            where stcd in {self.stcds}
                and date between (select distinct date from stock_price
                        where date < '{self.date}'
                        order by date desc offset 239 limit 1)
                    and '{self.aft_4d}'
                and volume != 0
            order by stcd desc, date desc
            """

        self.db.cursor.execute(query)
        columns = ["date", "stcd", "open",
                           "high", "low", "close", "values"]
        self.df = pd.DataFrame(self.db.cursor.fetchall(),
                               columns=columns)[::-1].reset_index(drop=True)

    def condition(self, df):
        try:
            # 10 거래일 평균 거래대금
            mean_values_10 = df["values"].rolling(window=10).mean()
            # 전일 종가
            prev_close = df["close"].shift(1)
            # 이동평균선
            df["ma19"] = df["close"].rolling(window=19).mean()
            df["ma112"] = df["close"].rolling(window=112).mean()
            df["ma224"] = df["close"].rolling(window=224).mean()
            df["sup1"] = df[["ma19", "ma112", "ma224"]].min(axis=1)
            # 조건1: 10 거래일 평균 거래대금 대비 10배 상승
            df["cond1"] = mean_values_10.shift(1) * 10 < df["values"]
            # 조건2: 최근 40일 조건1 을 충족한 적이 없어야함
            df["cond2"] = df["cond1"].shift(1).rolling(window=40).sum() == 0
            # 조건3: 이평선 밴드 폭이 좁아야 함(10 %)
            df["cond3"] = abs(df["ma112"] - df["ma224"]) * \
                2 / (df["ma112"] + df["ma224"]) < 0.1
            # 조건4: 5% 이상 양봉
            df["cond4"] = (df["close"] / prev_close >= 1.05) &\
                (df["open"] < df["close"])
            # 조건5: 해당일 224일선 돌파
            df["cond5"] = (df["sup1"] < df["close"]) & (
                (df["sup1"] > df["low"]) | (df["sup1"] > prev_close))
            ###
            if (
                (df[df["date"] == self.date]["cond1"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond2"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond3"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond4"].values[0] is np.True_) and
                (df[df["date"] == self.date]["cond5"].values[0] is np.True_)
            ):
                return df
            return None

        except:
            return None

    def price(self, df, weight):
        if weight >= 1:
            buy = df[df["date"] == self.aft_1d]["open"].values[0]
        if weight >= 2:
            buy += df[df["date"] == self.aft_2d]["open"].values[0]
        if weight >= 3:
            buy += df[df["date"] == self.aft_3d]["open"].values[0]
        buy_price = buy / weight

        profit_price = buy_price * (1 + self.target_rate)
        loss_price = buy_price * (1 - (self.target_rate))
        return [buy_price, profit_price, loss_price]

    def calculate(self, df):
        weight = 1
        buy_price, profit_price, loss_price = self.price(df, weight)

        if df[df["date"] == self.aft_1d]["low"].values[0] < loss_price:
            return (self.date, self.stcd, buy_price, weight, "f", loss_price)
        if df[df["date"] == self.aft_1d]["high"].values[0] > profit_price:
            return (self.date, self.stcd, buy_price, weight, "t", profit_price)
        if (df[df["date"] == self.aft_1d]["close"].values[0] < loss_price) and \
                (df[df["date"] == self.date]["volume"].values[0] > df[df["date"] == self.aft_1d]["volume"].values[0] * 0.5):
            return (self.date, self.stcd, buy_price, weight, "f", df[df["date"] == self.aft_1d]["close"].values[0])
        else:
            weight = 2
            buy_price, profit_price, loss_price = self.price(df, weight)

        if df[df["date"] == self.aft_2d]["low"].values[0] < loss_price:
            return (self.date, self.stcd, buy_price, weight, "f", loss_price)
        if df[df["date"] == self.aft_2d]["high"].values[0] > profit_price:
            return (self.date, self.stcd, buy_price, weight, "t", profit_price)
        if df[df["date"] == self.aft_2d]["close"].values[0] < loss_price:
            return (self.date, self.stcd, buy_price, weight, "f", df[df["date"] == self.aft_2d]["close"].values[0])
        else:
            weight = 3
            buy_price, profit_price, loss_price = self.price(df, weight)

        if df[df["date"] == self.aft_3d]["low"].values[0] < loss_price:
            return (self.date, self.stcd, buy_price, weight, "f", loss_price)
        if df[df["date"] == self.aft_3d]["high"].values[0] > profit_price:
            return (self.date, self.stcd, buy_price, weight, "t", profit_price)
        else:
            if df[df["date"] == self.aft_3d]["close"].values[0] > buy_price:
                return (self.date, self.stcd, buy_price, weight, "t", df[df["date"] == self.aft_3d]["close"].values[0])
            else:
                return (self.date, self.stcd, buy_price, weight, "f", df[df["date"] == self.aft_3d]["close"].values[0])

    def backtest(self):
        self.db_save = []
        for idx in range(len(self.stcds)):
            if idx % self.denom == 0 and idx // self.denom <= 10:
                print(f"{idx // self.denom} / 10")
            self.stcd = self.stcds[idx]
            tmp_df = self.df[self.df["stcd"] == self.stcds[idx]].copy()
            cond_df = self.condition(tmp_df)

            if cond_df is not None:
                calc = self.calculate(cond_df)
                self.db_save.append(calc)

        if len(self.db_save) != 0:
            self.db.multiInsertDB("backtest_st_002_2", self.db_save)
