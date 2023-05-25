import sys
from PyQt5.QtWidgets import QApplication
from api.kiwoom import Kiwoom


class Main():
    def __init__(self):
        self.kiwoom = Kiwoom()
        self.kiwoom.qm_st_001()
        # self.kiwoom.qm_st_002()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    main = Main()
    # app.exec_()
