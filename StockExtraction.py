import pandas as pd

from GetData import GetData


class StockExtraction:

    def make_factor(self, period):
        """특정 분기의 팩터를 만드는 함수 (2021Q2~)
        Parameters
        ==========
        period: str, 분기 (ex) '2022Q1'
        """
        period_q = period
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
        df_trailing = pd.DataFrame(gd.get_trailing(period=period))
        df_price = pd.DataFrame(gd.get_price(term=period_q))
        df = pd.merge(df_trailing, df_price, left_on='stock_code', right_on='code')

        fin_unit = 100000000
        df_factor[['stock_code', 'period']] = df[['stock_code', 'period_x']]
        df_factor['당기순이익'] = df['당기순이익']
        df_factor['list_shrs'] = df['list_shrs']
        df_factor['close'] = df['close']
        df_factor['EPS'] = (df['당기순이익'] * fin_unit) / df['list_shrs']
        df_factor['PER'] = df['close'] / df_factor['EPS']
        df_factor['BPS'] = (df['자본'] * fin_unit) / df['list_shrs']
        df_factor['PBR'] = df['close'] / df_factor['BPS']

        return df_factor


if __name__ == '__main__':
    se = StockExtraction()
    pd.set_option('display.max_rows', None, 'display.max_columns', None,
                  'display.width', None, 'display.max_colwidth', None)
    df_factor = se.make_factor('2022Q1')
    print(df_factor)
