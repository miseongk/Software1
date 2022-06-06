from sqlalchemy import create_engine
import json
import pandas as pd

from DBUpdater import DBUpdater


class GetData:

    def __init__(self):
        """생성자: DB 연결 및 종목코드 딕셔너리 생성"""
        db = json.loads(open('dbInfo.json', 'r').read())
        self.engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(db['user'], db['password'],
                                                                                      db['server'], db['database']))
        self.codes = dict()

    def __del__(self):
        """소멸자: DB 연결 해제"""
        self.engine.dispose()

    def read_all_stock_code(self):
        """상장 종목 코드 가져오기"""
        dbu = DBUpdater()
        dbu.update_comp_info()
        sql = "SELECT code, company FROM company_info"
        stock = pd.read_sql_query(sql, self.engine)

        return stock

    def get_is(self, stock_code=None, period=None):
        """손익계산서 가져오는 함수
        Parameters
        ==========
        stock_code: str, 종목 코드
            default: None (모든 종목 가져옴)
        period: str, 분기 (ex) '2022Q1'
            default: None (모든 분기 가져옴)
        """
        with self.engine.connect() as conn:
            if stock_code is None and period is None:
                sql = "SELECT * FROM krx_income_statement WHERE rpt_type='Consolidated_Q'"
            elif stock_code is None :
                sql = "SELECT * FROM krx_income_statement WHERE period='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(period)
            elif period is None:
                sql = "SELECT * FROM krx_income_statement WHERE stock_code='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(stock_code)
            else:
                sql = "SELECT * FROM krx_income_statement WHERE stock_code='{}' AND period='{}' " \
                    "AND rpt_type='Consolidated_Q'".format(stock_code, period)
            df_is = pd.read_sql_query(sql, conn)

        df_is = df_is.rename(columns={
            'revenue': '매출액',
            'cost_of_goods_sold': '매출원가',
            'gross_profit': '매출총이익',
            'sales_general_administrative_exp_total': '판매비와 관리비',
            'operating_profit_total': '영업이익',
            'financial_income_total': '금융이익',
            'financial_costs_total': '금융원가',
            'other_income_total': '기타수익',
            'other_costs_total': '기타비용',
            'subsidiaries_jointVentures_pl_total': '종속기업,공동지배기업및관계기업관련손익',
            'ebit': '세전계속사업이익',
            'income_taxes_exp': '법인세비용',
            'profit_cont_operation': '계속영업이익',
            'profit_discont_operation': '중단영업이익',
            'net_income_total': '당기순이익',
            'net_income_controlling': '지배주주순이익',
            'net_income_noncontrolling': '비지배주주순이익'
        })

        df_is = df_is[['stock_code', 'period',
                       '매출액', '매출원가', '매출총이익', '판매비와 관리비', '영업이익',
                       '금융이익', '금융원가', '기타수익', '기타비용', '종속기업,공동지배기업및관계기업관련손익',
                       '세전계속사업이익', '법인세비용', '계속영업이익', '중단영업이익',
                       '당기순이익', '지배주주순이익', '비지배주주순이익', 'rpt_type']]

        return df_is

    def get_bs(self, stock_code=None, period=None):
        """재무상태표 가져오는 함수
        Parameters
        ==========
        stock_code: str, 종목 코드
            default: None (모든 종목 가져옴)
        period: str, 분기 (ex) '2022Q1'
            default: None (모든 분기 가져옴)
        """
        with self.engine.connect() as conn:
            if stock_code is None and period is None:
                sql = "SELECT * FROM krx_balance_sheet WHERE rpt_type='Consolidated_Q'"
            elif stock_code is None :
                sql = "SELECT * FROM krx_balance_sheet WHERE period='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(period)
            elif period is None:
                sql = "SELECT * FROM krx_balance_sheet WHERE stock_code='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(stock_code)
            else:
                sql = "SELECT * FROM krx_balance_sheet WHERE stock_code='{}' AND period='{}' " \
                    "AND rpt_type='Consolidated_Q'".format(stock_code, period)
            df_bs = pd.read_sql_query(sql, conn)

        df_bs = df_bs.rename(columns={
            'assets_total': '자산',
            'current_assets_total': '유동자산',
            'lt_assets_total': '비유동자산',
            'other_fin_assets': '기타금융업자산',
            'liabilities_total': '부채',
            'current_liab_total': '유동부채',
            'lt_liab_total': '비유동부채',
            'other_fin_liab_total': '기타금융업부채',
            'equity_total': '자본',
            'paid_in_capital': '자본금',
            'contingent_convertible_bonds': '신종자본증권',
            'capital_surplus': '자본잉여금',
            'other_equity': '기타자본',
            'accum_other_comprehensive_income': '기타포괄손익누계액',
            'retained_earnings': '이익잉여금(결손금)'
        })

        df_bs = df_bs[['stock_code', 'period',
                       '자산', '유동자산', '비유동자산', '기타금융업자산',
                       '부채', '유동부채', '비유동부채', '기타금융업부채',
                       '자본', '자본금', '신종자본증권', '자본잉여금',
                       '기타자본', '기타포괄손익누계액', '이익잉여금(결손금)', 'rpt_type']]

        return df_bs

    def get_cf(self, stock_code=None, period=None):
        """현금흐름표 가져오는 함수
        Parameters
        ==========
        stock_code: str, 종목 코드
            default: None (모든 종목 가져옴)
        period: str, 분기 (ex) '2022Q1'
            default: None (모든 분기 가져옴)
        """
        with self.engine.connect() as conn:
            if stock_code is None and period is None:
                sql = "SELECT * FROM krx_cash_flow WHERE rpt_type='Consolidated_Q'"
            elif stock_code is None :
                sql = "SELECT * FROM krx_cash_flow WHERE period='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(period)
            elif period is None:
                sql = "SELECT * FROM krx_cash_flow WHERE stock_code='{}' " \
                      "AND rpt_type='Consolidated_Q'".format(stock_code)
            else:
                sql = "SELECT * FROM krx_cash_flow WHERE stock_code='{}' AND period='{}' " \
                    "AND rpt_type='Consolidated_Q'".format(stock_code, period)
            df_cf = pd.read_sql_query(sql, conn)

        df_cf = df_cf.rename(columns={
            'cfo_total': '영업활동으로인한현금흐름',
            'net_income_total': '당기순손익',
            'cont_biz_before_tax': '법인세비용차감전계속사업이익',
            'add_exp_wo_cf_out': '현금유출이없는비용등가산',
            'ded_rev_wo_cf_in': '(현금유입이없는수익등차감)',
            'chg_working_capital': '영업활동으로인한자산부채변동(운전자본변동)',
            'cfo': '*영업에서창출된현금흐름', 'other_cfo': '기타영업활동으로인한현금흐름',
            'cfi_total': '투자활동으로인한현금흐름',
            'cfi_in': '투자활동으로인한현금유입액',
            'cfi_out': '(투자활동으로인한현금유출액)',
            'other_cfi': '기타투자활동으로인한현금흐름',
            'cff_total': '재무활동으로인한현금흐름',
            'cff_in': '재무활동으로인한현금유입액',
            'cff_out': '(재무활동으로인한현금유출액)',
            'other_cff': '기타재무활동으로인한현금흐름',
            'other_cf': '영업투자재무활동기타현금흐름',
            'chg_cf_consolidation': '연결범위변동으로인한현금의증가',
            'forex_effect': '환율변동효과',
            'chg_cash_and_cash_equivalents': '현금및현금성자산의증가',
            'cash_and_cash_equivalents_beg': '기초현금및현금성자산',
            'cash_and_cash_equivalents_end': '기말현금및현금성자산'
        })

        df_cf = df_cf[['stock_code', 'period',
                       '영업활동으로인한현금흐름', '당기순손익', '법인세비용차감전계속사업이익', '현금유출이없는비용등가산',
                       '(현금유입이없는수익등차감)', '영업활동으로인한자산부채변동(운전자본변동)',
                       '*영업에서창출된현금흐름', '기타영업활동으로인한현금흐름',
                       '투자활동으로인한현금흐름', '투자활동으로인한현금유입액',
                       '(투자활동으로인한현금유출액)', '기타투자활동으로인한현금흐름',
                       '재무활동으로인한현금흐름', '재무활동으로인한현금유입액',
                       '(재무활동으로인한현금유출액)', '기타재무활동으로인한현금흐름',
                       '영업투자재무활동기타현금흐름', '연결범위변동으로인한현금의증가', '환율변동효과',
                       '현금및현금성자산의증가', '기초현금및현금성자산', '기말현금및현금성자산', 'rpt_type']]

        return df_cf


if __name__ == '__main__':
    data = GetData()
    df = pd.DataFrame(data.get_is('005930', '2022/03'))
    df.set_index('stock_code')
    pd.set_option('display.max_rows', None, 'display.max_columns', None,
                  'display.width', None, 'display.max_colwidth', None)
    print(df)

    df = pd.DataFrame(data.get_bs('005930', '2022/03'))
    df.set_index('stock_code')
    pd.set_option('display.max_rows', None, 'display.max_columns', None,
                  'display.width', None, 'display.max_colwidth', None)
    print(df)

    df = pd.DataFrame(data.get_cf('005930', '2022/03'))
    df.set_index('stock_code')
    pd.set_option('display.max_rows', None, 'display.max_columns', None,
                  'display.width', None, 'display.max_colwidth', None)
    print(df)




