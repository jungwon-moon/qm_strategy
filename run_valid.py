# 전략을 단순화 하여 과거 수익률을 검증함
from qm_strategy import QM_ST_002_VALID, QM_ST_003_VALID


def run_pre_search():

    # st_002 = QM_ST_002_VALID("20220103", "20221231")
    # st_002.valid()
    st_003 = QM_ST_003_VALID("20220103", "20221231")
    st_003.valid()


if __name__ == "__main__":
    run = run_pre_search()

