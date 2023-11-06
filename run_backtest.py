# 전략을 고도화 하여 실제 거래와 유사한 조건으로 백테스트를 수행함
from qm import utils
from qm_strategy import QM_ST_002_BACKTEST


def run_backtest():

    # st_001 = QM_ST_001_PRE_SEARCH()
    # st_001.search()
    yyyy = "2022"
    dates = utils.date_range(yyyy+"0101", yyyy+"0630")
    for date in dates:
        print(date)
        if utils.check_trading_day(date):
            st_002 = QM_ST_002_BACKTEST(date)
            st_002.backtest()


if __name__ == "__main__":
    run = run_backtest()
