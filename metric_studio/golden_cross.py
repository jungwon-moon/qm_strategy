import numpy as np
import pandas as pd
from qm import utils
from qm.connect import postgres_connect


def golden_cross(date=None):
    print(date)

    ago_1day = utils.change_date(date, "days", 1)
    aft_1week = utils.change_date(date, "weeks", 1)
    aft_2week = utils.change_date(date, "weeks", 2)
    aft_3week = utils.change_date(date, "weeks", 3)
    aft_1month = utils.change_date(date, "months", 1)
    aft_3month = utils.change_date(date, "months", 3)
    aft_6month = utils.change_date(date, "months", 6)

    def condition(df):
        try:
            df["ma20"] = df["close"].rolling(window=20).mean()
            df["ma60"] = df["close"].rolling(window=60).mean()
            df["cross"] = df["ma20"] < df["ma60"]

            if (
                (df[df["date"] == ago_1day]["cross"].values[0] is np.False_) &
                (df[df["date"] == date]["cross"].values[0] is np.True_)
            ):
                price = df[df["date"] == date]["close"].values[0]

                value = (
                    date,
                    "golden",
                    df["stcd"].values[0],
                    price,
                    df[df["date"] == aft_1week]["close"].values[0] / price - 1,
                    df[df["date"] == aft_2week]["close"].values[0] / price - 1,
                    df[df["date"] == aft_3week]["close"].values[0] / price - 1,
                    df[df["date"] == aft_1month]["close"].values[0] / price - 1,
                    df[df["date"] == aft_3month]["close"].values[0] / price - 1,
                    df[df["date"] == aft_6month]["close"].values[0] / price - 1,
                )
                return value
        except:
            return None

    db = postgres_connect()
    query = f"""
        select stcd from stock_price
        where date = '{date}'
        order by stcd
        """

    db.cursor.execute(query)
    stcds = tuple(map(lambda x: x[0], db.cursor.fetchall()))
    denom = len(stcds) // 10

    query = f"""
        select date, stcd, close, values
        from stock_price
        where stcd in {stcds}
            and date between (select distinct date from stock_price
                    where date < '{date}'
                    order by date desc offset 61 limit 1)
                and '{aft_6month}'
            and volume != 0
        order by stcd desc, date desc
        """

    db.cursor.execute(query)
    df = pd.DataFrame(db.cursor.fetchall())[::-1].reset_index(drop=True)
    df.columns = ["date", "stcd", "close", "values"]

    search_list = []
    for idx in range(len(stcds)):
        if idx % denom == 0 and idx // denom <= 10:
            print(f"{ idx // denom } / 10")

        tmp = df[df["stcd"] == stcds[idx]].copy()
        value = condition(tmp, "golden")

        if value is not None:
            search_list.append(value)

    print(len(search_list))
    if len(search_list) != 0:
        db.multiInsertDB("valid_cross_line", search_list)


yyyy = "2022"
dates = utils.date_range(yyyy + "0101", yyyy + "0429")

for date in dates:
    if utils.check_trading_day(date) is True:
        golden_cross(date)
        # golden_cross(date, "index")

# golden_cross('20220429')
