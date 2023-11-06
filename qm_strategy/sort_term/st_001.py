import numpy as np
import pandas as pd
from qm.connect import postgres_connect


class QM_ST_001():

    ### 전략 실행 함수 ###
    def qm_st_001(self):
        print("qm_st_001")
        self.st_001_screen("001")
        self.get_st_001_list()
        self.st_001_real_()

    def st_001_screen(self, num):
        num = int(num) * 1000
        self._st_001_screen = str(num)
        self._st_001_state = str(num + 100)
        self._st_001_real_screen = str(num + 200)
        self._st_001_trad_screen = str(num + 300)

    def get_st_001_list(self):
        print(f"st_001_list 불러오기: {self.db.host}")

    def st_001_real_(self):

        ### slot ###
        def real_data_slot(self, sCode, sRealType, sRealData):
            if sRealType == "장시작시간":
                fid = self.real_type.REALTYPE[sRealType]["장운영구분"]
                value = self.dynamicCall(
                    "GetCommRealData(str, int)", sCode, fid)
            print(value)
        ### set ###
        self.dynamicCall("SetRealReg(str, str, str, str)", self._st_001_state,
                         "", self.real_type.REALTYPE["장시작시간"]["장운영구분"], "0")

        self.OnReceiveRealData.connect(real_data_slot)


class QM_ST_001_PRE_SEARCH():

    def __init__(self):
        db = postgres_connect()
        query = f"""
            select stcd from stock_price 
            where date = (select max(date) from stock_price)
            order by stcd
            """
        # and values > 10000000000
        db.cursor.execute(query)
        self.stcds = tuple(map(lambda x: x[0], db.cursor.fetchall()))
        print(len(self.stcds))

        query = f"""
            select date, stcd, rate, prevd, open, high, low, close, values 
            from stock_price
            where stcd in {self.stcds}
                and date between (select distinct date from stock_price 
                        order by date desc offset 249 limit 1)
                    and (select max(date) from stock_price)
            order by stcd desc, date desc 
            """
        db.cursor.execute(query)
        self.result = db.cursor.fetchall()

    def condition(self, df):
        min_values_10 = df["values"].rolling(window=10).min()
        prev_close = df["close"].shift(1)
        df["ma19"] = df["close"].rolling(window=19).mean()
        df["ma112"] = df["close"].rolling(window=112).mean()
        df["ma224"] = df["close"].rolling(window=224).mean()
        # 1차 지지선
        df["sup_line1"] = df[["ma19", "ma112", "ma224"]].max(axis=1)
        # 10 거래일 최저 거래대금 대비 10배 상승
        df["cond1"] = min_values_10 * 10 < df["values"]
        # 5% 이상 양봉 확인
        df["cond2"] = df["rate"] > 5
        # 지지선 터치
        df["cond3"] = (df["sup_line1"] < df["close"]) & (
            (df["sup_line1"] > df["low"]) | (df["sup_line1"] > prev_close))
        # print(df["cond1"].values[-1],
        #       df["cond2"].values[-1], df["cond3"].values[-1])

        current = 2
        if (
            (df["cond1"].values[-(current+1)] is np.True_) &
            (df["cond2"].values[-(current+1)] is np.True_) &
            (df["cond3"].values[-(current+1)] is np.True_)
        ):
            stcd = df["stcd"].values[-(current+1)]
            date = df["date"].values[-(current+1)]
            value = (stcd, date)
            return value

    def search(self):
        df = pd.DataFrame(self.result)[::-1].reset_index(drop=True)
        df.columns = ["date", "stcd", "rate", "prevd",
                      "open", "high", "low", "close", "values"]
        search_list = []
        # for idx in range(len(self.stcds[1:2])):
        for idx in range(len(self.stcds)):
            print(f"{idx+1}/{len(self.stcds)}")
            tmp = df[df["stcd"] == self.stcds[idx]].copy()
            value = self.condition(tmp)
            if value:
                search_list.append(value)

        print(len(search_list))
        print(search_list)
