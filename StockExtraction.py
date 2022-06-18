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

        # PER 계산
        df_factor['EPS'] = (df['당기순이익'] * fin_unit) / df['list_shrs']
        df_factor['PER'] = df['close'] / df_factor['EPS']

        # PBR 계산
        df_factor['BPS'] = (df['자본'] * fin_unit) / df['list_shrs']
        df_factor['PBR'] = df['close'] / df_factor['BPS']

        # PSR 계산
        df_factor['SPS'] = (df['매출액'] * fin_unit) / df['list_shrs']
        df_factor['PSR'] = df['close'] / df_factor['SPS']

        # PCR 계산
        df_factor['OCF'] = df['당기순이익'] - df['비지배주주순이익'] + df['현금유출이없는비용등가산']
        df_factor['CFPS'] = (df_factor['OCF'] * fin_unit) / df['list_shrs']
        df_factor['PCR'] = df['close'] / df_factor['CFPS']

        # 가치지표 결합하기
        # 상대점수 설정
        score = 100 / len(df_factor)
        # PER이 낮은 순서대로 정렬
        df_per_sort = df_factor[['stock_code', 'PER']].sort_values(by='PER', ascending=False)
        df_per_sort = df_per_sort.reset_index(drop=True)
        # PER이 낮은 순서대로 상대점수 100점부터 0점까지 차등 분배
        df_per_sort['PER_Score'] = (df_per_sort.index.values + 1) * score
        # PBR이 낮은 순서대로 정렬
        df_pbr_sort = df_factor[['stock_code', 'PBR']].sort_values(by='PBR', ascending=False)
        df_pbr_sort = df_pbr_sort.reset_index(drop=True)
        # PBR이 낮은 순서대로 상대점수 100점부터 0점까지 차등 분배
        df_pbr_sort['PBR_Score'] = (df_pbr_sort.index.values + 1) * score
        # df_factor와 합치기
        df_factor = pd.merge(df_factor, df_per_sort, left_on=['stock_code', 'PER'], right_on=['stock_code', 'PER'])
        df_factor = pd.merge(df_factor, df_pbr_sort, left_on=['stock_code', 'PBR'], right_on=['stock_code', 'PBR'])
        # 가치지표 점수 계산
        df_factor['Combine_Score_PER_PBR'] = (df_factor['PER_Score'] + df_factor['PBR_Score']) / 2

        # 4대장 콤보
        # PSR이 낮은 순서대로 정렬
        df_psr_sort = df_factor[['stock_code', 'PSR']].sort_values(by='PSR', ascending=False)
        df_psr_sort = df_psr_sort.reset_index(drop=True)
        # PSR이 낮은 순서대로 상대점수 100점부터 0점까지 차등 분배
        df_psr_sort['PSR_Score'] = (df_psr_sort.index.values + 1) * score
        # PCR이 낮은 순서대로 정렬
        df_pcr_sort = df_factor[['stock_code', 'PCR']].sort_values(by='PCR', ascending=False)
        df_pcr_sort = df_pcr_sort.reset_index(drop=True)
        # PCR이 낮은 순서대로 상대점수 100점부터 0점까지 차등 분배
        df_pcr_sort['PCR_Score'] = (df_pcr_sort.index.values + 1) * score
        # df_factor와 합치기
        df_factor = pd.merge(df_factor, df_psr_sort, left_on=['stock_code', 'PSR'], right_on=['stock_code', 'PSR'])
        df_factor = pd.merge(df_factor, df_pcr_sort, left_on=['stock_code', 'PCR'], right_on=['stock_code', 'PCR'])
        # 4대장 콤보 점수 계산
        df_factor['4Combo'] = (df_factor['PER_Score'] + df_factor['PBR_Score']
                               + df_factor['PSR_Score'] + df_factor['PCR_Score']) / 4

        # 실적 대비 기업가치, EV/EBITDA 계산
        df_factor['EV'] = df['mktcap'] - df['부채'] + df['기말현금및현금성자산']
        df_factor['EBITDA'] = df['영업이익'] + df['현금유출이없는비용등가산']
        df_factor['EV/EBITDA'] = df_factor['EV'] / df_factor['EBITDA']

        # NCAV 계산
        df_factor['NCAV'] = df['유동자산'] - df['부채']
        df_factor['Safety_Margin'] = df_factor['NCAV'] - (df['mktcap'] * 1.5)

        # ROA 계산
        df_factor['ROA'] = df['당기순이익'] / df['자산']

        # ROE 계산
        df_factor['ROE'] = df['당기순이익'] / df['자본']

        # GP/A 계산
        df_factor['GP/A'] = df['매출총이익'] / df['자산']

        # 부채 비율 계산
        df_factor['Liability/Equity'] = df['부채'] / df['자본']

        # 차입금 비율 계산
        df_factor['Debt/Equity'] = (df['유동부채'] + df['비유동부채']) / df['자본']

        # 회전률 지표 계산
        df_factor['Assets_Turnover'] = df['매출액'] / df['자산']

        # 이익률 지표 계산
        df_factor['Gross_Margin'] = df['매출총이익'] / df['매출액']
        df_factor['Operating_Margin'] = df['영업이익'] / df['매출액']
        df_factor['Profit_Margin'] = df['당기순이익'] / df['매출액']

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

        # 회사명 가져오기
        data = GetData()
        stock = data.read_all_stock_code()

        # 종목과 회사명 합쳐서 데이터프레임으로 변환
        stock_select = pd.merge(df_select['stock_code'], stock, left_on='stock_code', right_on='code')
        stock_select = stock_select.drop(['code'], axis=1)

        return stock_select
