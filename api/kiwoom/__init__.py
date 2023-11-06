from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from api.kiwoom.errors import login_error
from api.kiwoom.type import RealType
from qm.connect import postgres_connect, kiwoom_info
from qm_strategy import QM_ST_001, QM_ST_002_TRADE


class Kiwoom(QAxWidget, QM_ST_001, QM_ST_002_TRADE):

    def __init__(self):
        super().__init__()
        # #
        self.real_type = RealType()
        self.db = postgres_connect()
        [self.user_pw, self.user_account] = kiwoom_info.values()
        # #
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        # 실행 함수 #
        self.login_func()
        self.get_connect_state_func()

    ### login_ ###
    def login_func(self):
        self.login_event_loop = QEventLoop()
        self.OnEventConnect.connect(self.login_slot)
        self.login_event()

    def login_slot(self, nErrCode):
        login_code = login_error(nErrCode)
        print(login_code)
        # 로그인 == 자동 매매 실행
        self.login_event_loop.exit()

    def login_event(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    ## get_connect_state ###
    def get_connect_state_func(self):
        state = self.dynamicCall("GetConnectState()")
        if state == 1:
            msg = "연결됨"
        elif state == 0:
            msg = "연결안됨"
        # 서버와의 연결 상태 슬랙에 전송
        print(msg)

# class Kiwoom(QAxWidget):

#     def __init__(self):
#         super().__init__()
#         ### Variable ###
#         self._pw = 1486
#         self._use_money = 100000

#         self._screen = "2000"
#         self._screen_calc = "4000"
#         self._screen_real_stock = "5000"
#         self._screen_trad_stock = "6000"
#         self._screen_state_market = "1000"

#         self.real_type = RealType()

#         ### Setting ###
#         self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
#         self.dynamicCall("SetRealReg(str, str, str, str)", self._screen_state_market,
#                          "", self.real_type.REALTYPE["장시작시간"]["장운영구분"], "0")

#         ### Event Loop###
#         self.login_event_loop = QEventLoop()
#         self.account_info_loop = QEventLoop()
#         self.account_mystock_loop = QEventLoop()
#         self.not_concluded_account_loop = QEventLoop()
#         self.day_kiwoom_db_loop = QEventLoop()
#         self.minute_candle_loop = QEventLoop()

#         ### Return Variable ###
#         self.login_code = None
#         self.acc_stock_dict = {}
#         self.portfolio_stock_dict = {}
#         self.read_stcd()        # 종목 불러오기

#         ### Slot(Handle) ###
#         self.OnEventConnect.connect(self.login_slot)
#         self.OnReceiveTrData.connect(self.trdata_slot)
#         self.real_event_slots()

#         ### Connect Event ###
#         self.login_event()
#         self.get_account_info()
#         self.detail_account_info()
#         self.detail_account_mystock()
#         self.not_concluded_account()

#         ### ###
#         # self.calculator_fnc()     # 의미없는 종목일봉 가져와서 계산..
#         self.show_minute_candle()
#         # self.minute_candle()
#         self.screen_number_setting()        # 스크린 번호 할당

#         ### real Data ###
#         # self.real_chejan_event()
#         # self.real_price_event()
#         ####################

#     def real_price_event(self):
#         for stcd in self.portfolio_stock_dict.keys():
#             screen_num = self.portfolio_stock_dict[stcd]["스크린번호"]
#             fids = self.real_type.REALTYPE["주식체결"]["현재가"]
#             self.dynamicCall("SetRealReg(str, str, str, str)",
#                              screen_num, stcd, fids, "1")
#             print(
#                 f"real_price 실시간 등록 코드 {stcd} 스크린번호: {screen_num} fid: {fids}")

#     def real_chejan_event(self):
#         for stcd in self.portfolio_stock_dict.keys():
#             screen_num = self.portfolio_stock_dict[stcd]["스크린번호"]
#             fids = self.real_type.REALTYPE["주식체결"]["체결시간"]
#             self.dynamicCall("SetRealReg(str, str, str, str)",
#                              screen_num, stcd, fids, "1")
#             print(
#                 f"real_chejan 실시간 등록 코드 {stcd} 스크린번호: {screen_num} fid: {fids}")

#     def real_event_slots(self):
#         self.OnReceiveRealData.connect(self.realdata_slot)

#     def login_event(self):
#         self.dynamicCall("CommConnect()")
#         self.login_event_loop.exec_()

#     def login_slot(self, nErrCode):
#         self.login_code = login_error(nErrCode)
#         print(self.login_code)
#         self.login_event_loop.exit()

#     def trdata_slot(self, sScrNo, sRQName, sTrCode, sRecordName, sPrevNext):
#         if sRQName == "예수금상세현황요청":
#             deposit = int(self.dynamicCall("GetCommData(str, str, int, str)",
#                                            sTrCode, sRQName, 0, "예수금"))
#             withdraw = int(self.dynamicCall("GetCommData(str, str, int, str)",
#                                             sTrCode, sRQName, 0, "출금가능금액"))

#             print(f"예수금: {deposit}")
#             print(f"출금가능: {withdraw}")

#             self.account_info_loop.exit()

#         if sRQName == "계좌평가잔고내역요청":
#             total_buy_money = int(self.dynamicCall(
#                 "GetCommData(str, str, int, str)", sTrCode, sRQName, 0, "총매입금액"))
#             total_profit_rate = float(self.dynamicCall(
#                 "GetCommData(str, str, int, str)", sTrCode, sRQName, 0, "총수익률(%)"))
#             cnt = self.dynamicCall(
#                 "GetRepeatCnt(str, str)", sTrCode, sRQName)

#             for i in range(cnt):
#                 stcd = self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "종목번호").strip()[1:]
#                 stnm = self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "종목명").strip()
#                 size = int(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "보유수량"))
#                 buy_price = int(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "매입가"))
#                 yield_rate = float(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "수익률(%)"))
#                 cur_price = int(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "현재가"))
#                 total_price = int(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "매입금액"))
#                 tradable_size = int(self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, i, "매매가능수량"))
#                 self.acc_stock_dict[stcd] = {
#                     "stcd": stcd,
#                     "stnm": stnm,
#                     "size": size,
#                     "buy_price": buy_price,
#                     "yield_rate": yield_rate,
#                     "cur_price": cur_price,
#                     "total_price": total_price,
#                     "tradable_size": tradable_size,
#                 }

#             print(f"보유종목 수: {cnt}")
#             print(f"총매입금액: {total_buy_money}")
#             print(f"총수익률(%): {total_profit_rate}%")
#             print(self.acc_stock_dict.keys())

#             self.account_mystock_loop.exit()

#         if sRQName == "실시간미체결요청":
#             cnt = self.dynamicCall("GetRepeatCnt(str, str)", sTrCode, sRQName)
#             for i in range(cnt):
#                 stcd = self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, 0, "종목코드").strip()[1:]
#                 order_num = self.dynamicCall(
#                     "GetCommData(str, str, int, str)", sTrCode, sRQName, 0, "주문번호")

#             self.not_concluded_account_loop.exit()

#         # 주식 일봉 차트 조회
#         if sTrCode == "OPT10081":
#             # ["", "현재가", "거래량", "거래대금", "날짜", "시가", "고가", "저가", ""]
#             data = self.dynamicCall(
#                 "GetCommDataEx(str, str)", sTrCode, sRQName)
#             print(data)
#             self.day_kiwoom_db_loop.exit()

#         if sTrCode == "OPT10080":
#             data = self.dynamicCall(
#                 "GetCommDataEx(str, str)", sTrCode, sRQName)
#             print(data)

#             self.minute_candle_loop.exit()

#     def realdata_slot(self, sCode, sRealType, sRealData):
#         print(sRealType, sCode)
#         if sRealType == "장시작시간":
#             fid = self.real_type.REALTYPE[sRealType]["장운영구분"]
#             value = self.dynamicCall("GetCommRealData(str, int)", sCode, fid)

#             if value == "0":
#                 print("장 시작 전")
#             elif value == "2":
#                 print("장 종료, 동시호가")
#             elif value == "3":
#                 print("장 시작")
#             elif value == "4":
#                 print("장 종료")

#         if sRealType == "주식체결":
#             fid = self.real_type.REALTYPE[sRealType]["현재가"]
#             value = self.dynamicCall("GetCommRealData(str, int)", sCode, fid)
#             print(f"{sCode} 현재가: {value}")

#         if sRealType == "현재가":
#             fid = self.real_type.REALTYPE[sRealType]["현재가"]
#             value = self.dynamicCall("GetCommRealData(str, int)", sCode, fid)
#             print(f"{sCode} 현재가: {value}")

#     def get_account_info(self):
#         self.server = self.dynamicCall("GetLoginInfo(str)", "GetServerGubun")
#         self.key_sec = self.dynamicCall("GetLoginInfo(str)", "KEY_BSECGB")
#         self.fire_sec = self.dynamicCall("GetLoginInfo(str)", "FIREW_SECGB")
#         self.acc_cnt = self.dynamicCall("GetLoginInfo(str)", "ACCOUNT_CNT")
#         self.acc_list = self.dynamicCall("GetLoginInfo(str)", "ACCLIST")
#         self.use_acc = self.acc_list.split(";")[0]
#         self.usr_name = self.dynamicCall("GetLoginInfo(str)", "USER_NAME")

#         # print(f"서버구분: {self.server}")
#         # print(f"키보드 보안: {self.key_sec}")
#         # print(f"방화벽: {self.fire_sec}")
#         print(f"계좌 개수: {self.acc_cnt}")
#         print(f"계좌 번호: {self.use_acc}")
#         # print(f"사용자 이름: {self.usr_name}")

#     def detail_account_info(self, sPrevNext="0"):
#         self.dynamicCall("SetInputValue(str, str)", "계좌번호", self.use_acc)
#         self.dynamicCall("SetInputValue(str, str)", "비밀번호", self._pw)
#         self.dynamicCall("SetInputValue(str, str)", "비밀번호입력매체구분", "00")
#         self.dynamicCall("SetInputValue(str, str)", "조회구분", "2")
#         self.dynamicCall("CommRqData(str, str, str, str)",
#                          "예수금상세현황요청", "OPW00001", sPrevNext, self._screen)
#         self.account_info_loop.exec_()

#     def detail_account_mystock(self, sPrevNext="0"):
#         self.dynamicCall("SetInputValue(str, str)", "계좌번호", self.use_acc)
#         self.dynamicCall("SetInputValue(str, str)", "비밀번호", self._pw)
#         self.dynamicCall("SetInputValue(str, str)", "비밀번호입력매체구분", "00")
#         self.dynamicCall("SetInputValue(str, str)", "조회구분", "2")
#         self.dynamicCall("CommRqData(str, str, str, str)",
#                          "계좌평가잔고내역요청", "OPW00018", sPrevNext, self._screen)
#         self.account_mystock_loop.exec_()

#     def not_concluded_account(self, sPrevNext="0"):
#         self.dynamicCall("SetInputValue(str, str)", "계좌번호", self.use_acc)
#         self.dynamicCall("SetInputValue(str, str)", "체결구분", "1")
#         self.dynamicCall("SetInputValue(str, str)", "매매구분", "0")
#         self.dynamicCall("CommRqData(str, str, str, str)",
#                          "실시간미체결요청", "OPT10075", sPrevNext, self._screen)
#         self.not_concluded_account_loop.exec_()

#     def get_stcd_list(self, market_code):
#         stcd_list = self.dynamicCall(
#             "GetCodeListByMarket(str)", market_code).split(";")[:-1]
#         return stcd_list

#     def calculator_fnc(self):
#         stcd_list = self.get_stcd_list("10")
#         print(f"코스닥 개수 {len(stcd_list)}")

#         for idx, code in enumerate(stcd_list):
#             self.dynamicCall("DisconnectRealData(QString)", self._screen_calc)
#             print(f"{idx+1} / {len(stcd_list)} {code} is updating...")
#             self.day_kiwoom_db(code=code)
#             # self.minute_candle(code=code)

#     def show_minute_candle(self):
#         print("show_minute_candle")
#         print(self.portfolio_stock_dict)
#         for stcd in self.portfolio_stock_dict.keys():
#             print(stcd)
#             self.minute_candle(code=stcd)

#     def day_kiwoom_db(self, code=None, date=None, sPrevNext="0"):

#         QTest.qWait(500)

#         self.dynamicCall("SetInputValue(str, str)", "종목코드", code)
#         self.dynamicCall("SetInputValue(str, str)", "수정주가구분", "1")
#         if date is not None:
#             self.dynamicCall("SetInputValue(str, str)", "기준일자", date)
#         self.dynamicCall("CommRqData(str, str, int, str)",
#                          "주식일봉차트조회", "OPT10081", sPrevNext, self._screen_calc)
#         if sPrevNext == "0":
#             self.day_kiwoom_db_loop.exec_()

#     def minute_candle(self, code=None, sPrevNext="0"):
#         QTest.qWait(500)
#         self.dynamicCall("SetInputValue(str, str)", "종목코드", code)
#         self.dynamicCall("SetInputValue(str, str)", "틱범위", "3")
#         self.dynamicCall("SetInputValue(str, str)", "수정주가구분", "1")
#         self.dynamicCall("CommRqData(str, str, int, str)",
#                          "주식일봉차트조회", "OPT10080", sPrevNext, self._screen_calc)
#         self.minute_candle_loop.exec_()

#     def read_stcd(self):
#         self.portfolio_stock_dict = {
#             "005930": {"종목명": "삼성", "현재가": "60000"},
#             "001340": {"종목명": "몰루1", "현재가": "60000"},
#             "000660": {"종목명": "몰루2", "현재가": "60000"},
#             "001470": {"종목명": "몰루3", "현재가": "60000"},
#             "001790": {"종목명": "몰루4", "현재가": "60000"},
#         }

#     def screen_number_setting(self):

#         screen_overwrite = []

#         for stcd in self.acc_stock_dict.keys():
#             if stcd not in screen_overwrite:
#                 screen_overwrite.append(stcd)

#         for stcd in self.portfolio_stock_dict.keys():
#             if stcd not in screen_overwrite:
#                 screen_overwrite.append(stcd)

#         # 스크린번호 할당
#         cnt = 0
#         for stcd in screen_overwrite:
#             real_screen = int(self._screen_real_stock)
#             trad_screen = int(self._screen_trad_stock)

#             if (cnt % 50) == 0:
#                 real_screen += 1
#                 trad_screen += 1
#                 self._screen_real_stock = str(real_screen)
#                 self._screen_trad_stock = str(trad_screen)

#             if stcd in self.portfolio_stock_dict.keys():
#                 self.portfolio_stock_dict[stcd].update(
#                     {
#                         "스크린번호": str(real_screen),
#                         "주문용스크린번호": str(trad_screen)
#                     })
#             cnt += 1

#         print(self.portfolio_stock_dict)
