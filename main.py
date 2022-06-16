import pandas as pd

from DBUpdater import DBUpdater
from GetData import GetData
from StockExtraction import StockExtraction

if __name__ == '__main__':
    pd.set_option('mode.chained_assignment', None)

    dbu = DBUpdater()
    data = GetData()
    stock = StockExtraction()

