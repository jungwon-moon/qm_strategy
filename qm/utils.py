import requests
import time
import datetime
from dateutil.relativedelta import relativedelta


def dt2str(td: datetime.datetime, Type: str = "day") -> str:
    r"""
    datetime -> str.

    Type = "day" -> "yyyymmdd"
    """
    if Type == "day":
        return ''.join(filter(str.isalnum, str(td)))[:8]
    if Type == "time":
        return ''.join(filter(str.isalnum, str(td)))[:12]


def str2dt(td: str) -> datetime.datetime:
    r"""
    str -> datetime
    """
    return datetime.datetime.strptime(td, "%Y%m%d")


def ts2dt(td: str) -> datetime.datetime:
    r"""
    timestamp -> datetime
    """
    return datetime.datetime.fromtimestamp(td)


def str2ts(td: str):
    r"""
    str(datetime) -> timestamp
    """
    return int(time.mktime(datetime.datetime.strptime(td, '%Y-%m-%d %H:%M:%S').timetuple()))


_today = dt2str(datetime.datetime.today(), Type="day")
_time = dt2str(datetime.datetime.today(), Type="time")


def replace_zero(txt: str):
    txt = txt.replace(",", "")
    if txt == "-":
        return None
    return txt


def calc_roe(eps, bps):
    eps = replace_zero(eps)
    bps = replace_zero(bps)
    if eps == None or bps == None:
        return None
    return str(round(float(eps) / float(bps) * 100, 2))


def check_trading_day(td: str, db=None) -> bool:
    r"""
    지정일의 개장 여부 확인
    """
    td = dt2str(td)

    # 주말 확인
    # 0:월 ~ 6:일
    if datetime.date(int(td[:4]), int(td[4:6]), int(td[6:])).weekday() > 4:
        return False

    # 휴장일 확인
    # API로 확인
    if db == None:
        req_url = "https://quantmag.net/api/kr/holiday"
        response = requests.get(req_url).json()["results"]
        holiday = [row["calnd_dd"] for row in response]

        if td in holiday:
            return False

    else:
        holiday = [k[0] for k in db.readDB("holiday", "calnd_dd")]
        if td in holiday:
            return False

    return True


def date_range(start, end):
    start = datetime.datetime.strptime(start, "%Y%m%d")
    end = datetime.datetime.strptime(end, "%Y%m%d")
    dates = [(start + datetime.timedelta(days=i)).strftime("%Y%m%d")
             for i in range((end-start).days+1)]
    return dates


def change_date(dt, type, num):
    if type == "weeks":
        date = dt2str(str2dt(dt) + relativedelta(weeks=num))
    elif type == "months":
        date = dt2str(str2dt(dt) + relativedelta(months=num))
    elif type == "days":
        date = dt2str(str2dt(dt) + relativedelta(days=num))
    while (not check_trading_day(date)):
        if type == "days" and num < 0:
            date = dt2str(str2dt(date) - relativedelta(days=1))
        else:
            date = dt2str(str2dt(date) + relativedelta(days=1))
    return date
