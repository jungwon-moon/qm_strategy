from qm import utils
from binance.spot import Spot
from binance.um_futures import UMFutures


class BINANCE_INFO():
    def __init__(self, param):
        for key, value in param.items():
            if key == "api":
                self.api = value
            if key == "secret":
                self.secret = value

        try:
            # USD-M Futures
            self.client = UMFutures(key=self.api, secret=self.secret)
            print("serverTime: ", str(utils.ts2dt(
                self.client.time()["serverTime"] / 1000)))

        except Exception as e:
            print("BINANCE connection error: ", e)


class BINANCE(BINANCE_INFO):
    # # #
    def query(self, url_path: str):
        return self.client.query(url_path)

    # # #
    def exchange_info(self):
        return self.client.exchange_info()

    def get_account(self):
        return self.client.account()

    def get_balance(self):
        data = self.client.balance()
        return [k for k in data if float(k["balance"]) != 0]

    def get_funding_rate(self, symbol: str):
        data = self.client.funding_rate(symbol)
        results = list()
        for f in data:
            result = {
                "fundingTime": str(utils.ts2dt(f["fundingTime"] / 1000)),
                "fundingRate": f["fundingRate"],
                "markPrice": f["markPrice"]
            }
            results.append(result)
        return results

    def get_top_long_short_position_ratio(self, symbol: str, period: str):
        data = self.client.top_long_short_position_ratio(symbol, period)
        results = list()
        for r in data:
            result = {
                "time": str(utils.ts2dt(r["timestamp"] / 1000)),
                "longShortRatio": r["longShortRatio"],
                "longAccount": r["longAccount"],
                "shortAccount": r["shortAccount"]
            }
            results.append(result)
        return results

    def get_book_ticker(self, symbol: str = None):
        data = self.client.book_ticker(symbol=symbol)
        result = {
            "time": str(utils.ts2dt(data["time"] / 1000)),
            "bidPrice": data["bidPrice"],
            "askPrice": data["askPrice"]
        }
        return result

    def get_ticker_24hr_price_change(self, symbol: str):
        r"""
        24시간 가격 변동 정보
        weightedAvgPrice: 가중평균가
        quoteVolume: USDT 거래량
        volume: 해당 코인 거래량
        """
        data = self.client.ticker_24hr_price_change(symbol=symbol)
        result = {
            "symbol": data["symbol"],
            "openTime": str(utils.ts2dt(data["openTime"] / 1000)),
            "closeTime": str(utils.ts2dt(data["closeTime"] / 1000)),
            "weightedAvgPrice": data["weightedAvgPrice"],
            "lastPrice": data["lastPrice"],
            "quoteVolume": data["quoteVolume"],
            "volume": data["volume"],
            "priceChangePercent": data["priceChangePercent"],
            "lastQty": data["lastQty"]
        }
        return result

    def get_open_interest(self, symbol: str, period: str):
        """
        미결제 약정
        sumOpenInterest: 해당 코인
        sumOpenInterestValue: USDT
        """
        data = self.client.open_interest_hist(symbol=symbol, period=period)
        results = list()
        for d in data:
            result = {
                "symbol": d["symbol"],
                "timestamp": str(utils.ts2dt(d["timestamp"] / 1000)),
                "sumOpenInterest": d["sumOpenInterest"],
                "sumOpenInterestValue": d["sumOpenInterestValue"],
            }
            results.append(result)
        return results

    def get_ticker_price(self, symbol: str):
        data = self.client.ticker_price(symbol=symbol)
        return data

    def get_mark_price(self, symbol: str):
        r"""
        get_mark_price 

        markPrice,
        indexPrice

        """
        data = self.client.mark_price(symbol=symbol)
        return data

    def get_klines(self, symbol: str, interval: str, startTime: str = None, endTime: str = None,  limit: int = None):
        data = self.client.klines(
            symbol=symbol, interval=interval, startTime=startTime, endTime=endTime, limit=limit)
        return data

    # # #
    def get_symbols(self):
        symbols = self.exchange_info()["symbols"]
        return [k["symbol"] for k in symbols]

    def get_current_position(self):
        account = self.get_account()
        position = account["positions"]
        return [k for k in position if float(k["notional"]) != 0]
