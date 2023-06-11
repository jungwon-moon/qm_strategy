from qm_strategy import QM_ST_001_PRE_SEARCH, QM_ST_002_PER_SEARCH


def run_pre_search():

    # st_001 = QM_ST_001_PRE_SEARCH()
    # st_001.search()
    st_002 = QM_ST_002_PER_SEARCH("20230609")
    st_002.search()


if __name__ == "__main__":
    run = run_pre_search()
