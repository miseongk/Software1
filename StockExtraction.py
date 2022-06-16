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
        df_factor['mktcap'] = df['mktcap']
        df_factor['list_shrs'] = df['list_shrs']
        df_factor['close'] = df['close']
        df_factor['EPS'] = (df['당기순이익'] * fin_unit) / df['list_shrs']
        df_factor['PER'] = df['close'] / df_factor['EPS']
        df_factor['BPS'] = (df['자본'] * fin_unit) / df['list_shrs']
        df_factor['PBR'] = df['close'] / df_factor['BPS']

        return df_factor

    def stock_select(self, df_factor, MKTCAP_top, n, factor_list):
        """종목 추출 함수
        Parameters
        ==========
        df_factor: pd.DataFrame, 팩터
        MKTCAP_top: float, 시가총액 상위 퍼센트
        n: int, 종목 추출 개수
        factor_list: list, (ex) ['PER', 'PBR']
        """
        basic_list = ['stock_code', 'period', 'mktcap']

        basic_list.extend(factor_list)

        df_select = df_factor.copy()
        df_select = df_select[basic_list]

        df_select['score'] = 0

        # 시가총액 상위 MKTCAP_top% 산출
        df_select = df_select.sort_values(by=['mktcap'], ascending=False).head(int(len(df_select) * MKTCAP_top))
        df_select = df_select.dropna()

        # 팩터간의 점수 계산
        for i in range(len(factor_list)):
            df_select[factor_list[i] + '_score'] = (df_select[factor_list[i]] - max(df_select[factor_list[i]]))
            df_select[factor_list[i] + '_score'] = df_select[factor_list[i] + '_score'] / min(
                df_select[factor_list[i] + '_score'])

            df_select['score'] += (df_select[factor_list[i] + '_score'] / len(factor_list))

        # 상위 n개 종목 추출
        df_select = df_select.sort_values(by=['score'], ascending=False).head(n)

        # 종목 선택
        #stock_select = pd.DataFrame(df_select['stock_code'])

        # 회사명 가져오기
        # data = GetData()
        # stock = data.read_all_stock_code()
        stock_select = pd.DataFrame(df_select['stock_code'])
        # # 종목 선택
        #stock_select = list(df_select['stock_code'])

        return stock_select

if __name__ == '__main__':
    stock = StockExtraction()
    df_factor = stock.make_factor('2022Q1')
    MKTCAP_top = 3  # 시가총액 상위 3%
    n = 30  # 30개 종목 추출
    factor_list = ['PER', 'PBR']
    s = stock.stock_select(df_factor, MKTCAP_top, n, factor_list)
    print(s)
