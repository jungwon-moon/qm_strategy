import sys
from PyQt5.QtWidgets import QApplication
from api.kiwoom import Kiwoom


class Main():
    def __init__(self):
        self.kiwoom = Kiwoom()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main = Main()
    # app.exec_()
