import pandas as pd

from DBUpdater import DBUpdater
from GetData import GetData
from StockExtraction import StockExtraction

if __name__ == '__main__':
    pd.set_option('mode.chained_assignment', None)

    dbu = DBUpdater()
    data = GetData()
    stock = StockExtraction()

    df_factor = stock.make_factor('2022Q1')
    MKTCAP_top = 3  # 시가총액 상위 3%
    n = 30  # 30개 종목 추출
    factor_list = ['PER', 'PBR']
    # s = stock.stock_select(df_factor, MKTCAP_top, n, factor_list)
    # print(s)
    print(df_factor)

