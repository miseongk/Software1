import pandas as pd

from GetData import GetData


class StockExtraction:

    def make_factor(self, period):
        """특정 분기의 팩터를 만드는 함수 (2021Q2~)
        Parameters
        ==========
        period: str, 분기 (ex) '2022Q1'
        """
        if period[5] == '1':  # 1분기
            period = period[0:4] + '/03'
        elif period[5] == '2':  # 2분기
            period = period[0:4] + '/06'
        elif period[5] == '3':  # 3분기
            period = period[0:4] + '/09'
        elif period[5] == '4':  # 4분기
            period = period[0:4] + '/12'

        gd = GetData()
        gd.read_all_stock_code()
        df_factor = pd.DataFrame()
        # df_is = pd.DataFrame(gd.get_is(period=period))
        # df_bs = pd.DataFrame(gd.get_bs(period=period))
        # df_cf = pd.DataFrame(gd.get_cf(period=period))
        df_trailing = pd.DataFrame(gd.get_trailing(period=period))

        df_factor[['stock_code', 'period']] = df_trailing[['stock_code', 'period']]
        # df_factor['EPS'] = df_is['당기순이익'] / '상장주식수'

        return df_factor


if __name__ == '__main__':
    se = StockExtraction()
    df_factor = se.make_factor('2022Q1')
    print(df_factor)
