from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from api.kiwoom.errors import login_error


class Kiwoom(QAxWidget):

    def __init__(self):
        super().__init__()
        ### Set ###
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        ### Event Loop###
        self.login_event_loop = QEventLoop()
        self.account_info_loop = QEventLoop()

        ### Variable ###
        self.login_code = None
        self.acc_list = None

        ### Slot(Handle) ###
        self.event_slots()

        ### Connect Event ###
        self.default_event()

    def default_event(self):
        self.login_event()
        self.get_account_info()
        self.detail_account_info()

    def event_slots(self):
        self.OnEventConnect.connect(self.login_slot)
        self.OnReceiveTrData.connect(self.trdata_slot)

    def login_event(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_slot(self, nErrCode):
        self.login_code = login_error(nErrCode)
        self.login_event_loop.exit()

    def trdata_slot(self, sScrNo, sRQName, sTrCode, sRecordName, sPrevNext):
        if sRQName == "예수금상세현황요청":
            deposit = self.dynamicCall("GetCommData(str, str, int, str)",
                                       sTrCode, sRQName, 0, "예수금")
            withdraw = self.dynamicCall("GetCommData(str, str, int, str)",
                                        sTrCode, sRQName, 0, "출금가능금액")
            print(f"예수금: {deposit}")
            print(f"출금가능: {withdraw}")
            self.account_info_loop.exit()

    def get_account_info(self):
        self.server = self.dynamicCall("GetLoginInfo(str)", "GetServerGubun")
        self.key_sec = self.dynamicCall("GetLoginInfo(str)", "KEY_BSECGB")
        self.fire_sec = self.dynamicCall("GetLoginInfo(str)", "FIREW_SECGB")
        self.acc_cnt = self.dynamicCall("GetLoginInfo(str)", "ACCOUNT_CNT")
        self.acc_list = self.dynamicCall("GetLoginInfo(str)", "ACCLIST")
        self.use_acc = self.acc_list.split(";")[0]
        self.usr_name = self.dynamicCall("GetLoginInfo(str)", "USER_NAME")

        print(f"서버구분: {self.server}")
        print(f"키보드 보안: {self.key_sec}")
        print(f"방화벽: {self.fire_sec}")
        print(f"계좌 개수: {self.acc_cnt}")
        print(f"계좌 번호: {self.use_acc}")
        print(f"사용자 이름: {self.usr_name}")

    def detail_account_info(self, sPrevNext="0"):
        self.dynamicCall("SetInputValue(str, str)", "계좌번호", self.use_acc)
        self.dynamicCall("SetInputValue(str, str)", "비밀번호", "0000")
        self.dynamicCall("SetInputValue(str, str)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(str, str)", "조회구분", "2")
        self.dynamicCall("CommRqData(str, str, str, str)",
                         "예수금상세현황요청", "OPW00001", sPrevNext, "2000")
        self.account_info_loop.exec_()
