# 전략을 기반으로 실제 거래를 수행함.
import sys
from PyQt5.QtWidgets import QApplication
from api.kiwoom import Kiwoom


class Run_trade():
    def __init__(self):
        self.kiwoom = Kiwoom()
        # self.kiwoom2 = Kiwoom()
        # self.kiwoom.qm_st_001()
        # self.kiwoom2.qm_st_001()
        # self.kiwoom.qm_st_002()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    run = Run_trade()
    # app.exec_()
