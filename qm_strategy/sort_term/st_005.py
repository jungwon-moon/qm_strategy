import math
import time
import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect
import FinanceDataReader as fdr

s = time.time()


db = postgres_connect()

today = utils._today
today = "20230724"
ysday = utils.change_date(today, "days", -1)
start = utils.change_date(today, "months", -2)

stcds = fdr.StockListing("KRX")
stcds = stcds["Code"]

search_stock_list = list()
for idx in range(len(stcds)):
    # print(f"{idx} / {len(stcds)}")

    df = fdr.DataReader(stcds[idx], start=start, end=today)

    if len(df) <= 2:
        continue
    if utils.dt2str(df.index[-1]) != today:
        continue

    cond1 = df["Volume"].max() == df["Volume"][df.index == today].values[0]
    cond2 = df["Volume"][df.index == ysday].values[0] * \
        10 < df["Volume"][df.index == today].values[0]
    cond3 = df["Volume"][df.index == today].values[0] * \
        df["Close"][df.index == today].values[0] >= 10000000000
    cond4 = df["Close"][df.index == today].values[0] - \
        df["Open"][df.index == today].values[0] > 0

    if (
        (cond1 is np.True_) and
        (cond2 is np.True_) and
        (cond3 is np.True_) and
        (cond4 is np.True_)
    ):
        stcd = stcds[idx]
        body = df["Close"][df.index == today].values[0] - \
            df["Open"][df.index == today].values[0]
        poll = df["High"][df.index == today].values[0] - \
            df["Low"][df.index == today].values[0]
        bid = df["Close"][df.index == today].values[0]
        val = f"종목코드: {stcd} / Body: {body} ({round(100*body/bid, 2)} %)\n\
            진입가: {bid} / 익절가: {bid + (body//2)} \n\
            1차 손절: {bid - (body//2)} 2차 손절: {bid - body}"
        search_stock_list.append(val)

for stock in search_stock_list:
    print(stock)


print(f"===== {round(time.time() - s, 4)} s =====")
