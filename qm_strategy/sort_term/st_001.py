from PyQt5.QtCore import QEventLoop


class QM_ST_001():


    ### 전략 실행 함수 ###
    def qm_st_001(self):
        print("qm_st_001")
        self.create_screen_num("st", "001")
        self.get_st_001_list()
        self.st_001_real_()

    def create_screen_num(self, nm, num):
        globals()[f"{nm}_{num}"] = num
        

    def get_st_001_list(self):
        print(f"st_001_list 불러오기: {self.db.host}")

    def st_001_real_(self):
        print(st_001)
        ### loop ###
        self.st_001_real_loop = QEventLoop()
        ### slot ###

        ### event ###
