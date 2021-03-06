from sqlalchemy import create_engine
import json
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import requests
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request


class DBUpdater:

    def __init__(self):
        """생성자: DB 연결 및 종목코드 딕셔너리 생성"""
        db = json.loads(open('dbInfo.json', 'r').read())
        self.engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(db['user'], db['password'],
                                                                                 db['server'], db['database']))
        self.codes = dict()

    def __del__(self):
        """소멸자: DB 연결 해제"""
        self.engine.dispose()

    def read_krx_code(self):
        """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=' \
              'download&searchType=13'
        krx = pd.read_html(url, header=0, flavor='html5lib', encoding='euc-kr')[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)

        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql_query(sql, self.engine)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
        with self.engine.connect() as conn:
            # 가장 최신 업데이트 날짜 가져오기
            sql = "SELECT max(last_update) FROM company_info"
            rs = conn.execute(sql).first()
            today = datetime.today().strftime('%Y-%m-%d')

            # 테이블이 비어있거나 최신 날짜가 오늘보다 이전일 경우 업데이트
            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last_update)" \
                          f"VALUES ('{code}', '{company}', '{today}')"
                    conn.execute(sql)
                    self.codes[code] = company
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f"[{tmnow}] Update {len(krx)} company info Successfully! ")
            else:
                print("Already updated today.")

    def read_all_stock(self):
        """상장 종목 코드 가져오기"""
        self.update_comp_info()
        sql = "SELECT code, company FROM company_info"
        stock = pd.read_sql_query(sql, self.engine)

        return stock

    def get_price_by_date(self, date):
        """특정 날짜로 DB에서 시세 가져오기"""
        sql = f"SELECT * FROM daily_price WHERE date = '{date}'"
        price = pd.read_sql_query(sql, self.engine)

        return price

    def replace_into_daily_price_db(self, code, company, start, end):
        """주식 시세를 읽어서 DB에 업데이트
        Parameters
        ==========
        code: str, 종목코드
        company: str, 회사명
        start: str, 시작 날짜 (ex) '2022-01-01'
        end: str, 종료 날짜 (ex) '2022-06-08'"""
        daily = pd.DataFrame()
        # FDR 패키지로 시세 가져오기
        ohlcv = fdr.DataReader(code, start, end)
        ohlcv['Code'] = code
        ohlcv['Name'] = company
        daily = pd.concat([daily, ohlcv])
        daily = daily.dropna()
        if len(daily) == 0:
            return
        # daily_price 테이블에 저장
        with self.engine.connect() as conn:
            for r in daily.itertuples():
                sql = f"REPLACE INTO daily_price (open, high, low, close, volume, change_, code, name, date)" \
                      f"VALUES ({r.Open}, {r.High}, {r.Low}, {r.Close}, {r.Volume}, {r.Change}," \
                      f" '{r.Code}', '{r.Name}', '{r.Index.date()}')"
                conn.execute(sql)

    def replace_into_daily_price_db_extra_data(self, start, end):
        """모든 종목의 시가총액과 상장주식수를 krx에서 읽어와 DB에 추가로 업데이트
        Parameters
        ==========
        start: str, 시작 날짜 (ex) '2022-01-01'
        end: str, 종료 날짜 (ex) '2022-06-08'
        """
        # 수집 기간 설정
        sdate = datetime.strptime(start.replace('-', ''), '%Y%m%d').date()
        edate = datetime.strptime(end.replace('-', ''), '%Y%m%d').date()
        dt_idx = []
        for dt in self.get_date_range(sdate, edate):
            if dt.isoweekday() < 6:
                dt_idx.append(dt.strftime("%Y%m%d"))
        # 수집 기간 동안 반복
        for dt in dt_idx:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/101.0.4951.64 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }
            p_data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'locale': 'ko_KR',
                'mktId': 'ALL',
                'trdDd': dt,
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
            }
            url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
            # krx에서 데이터 가져옴
            res = requests.post(url, headers=headers, data=p_data)
            html_json = json.loads(res.content)
            html_json = html_json['OutBlock_1']

            name_hs = pd.DataFrame()
            if len(html_json) > 0:
                name_h = []
                for i in range(len(html_json)):
                    if html_json[i]['TDD_OPNPRC'] == '-':  # 시장이 열리지 않아 값이 없는 경우
                        continue
                    # 종목코드, 시가총액, 상장주식수만 뽑아냄
                    ISU_SRT_CD = html_json[i]['ISU_SRT_CD']
                    MKTCAP = int(html_json[i]['MKTCAP'].replace(',', ''))
                    LIST_SHRS = int(html_json[i]['LIST_SHRS'].replace(',', ''))
                    name_h.append((ISU_SRT_CD, MKTCAP, LIST_SHRS))

                name_h = pd.DataFrame(name_h, columns=['code', 'mktcap', 'list_shrs'])
                name_hs = pd.concat([name_hs, name_h], ignore_index=True)
            else:
                pass
            name_hs = name_hs.sort_values(by=['code'], axis=0)
            date = dt[0:4] + '-' + dt[4:6] + '-' + dt[6:8]
            # DB에 수집된 종목 시세 가져오기
            stock = self.get_price_by_date(date)
            stock = stock.drop(['mktcap', 'list_shrs'], axis=1)
            # krx에서 가져온 정보를 수집된 종목에 합침
            result = pd.merge(stock, name_hs, left_on='code', right_on='code')

            # DB에 업데이트
            with self.engine.connect() as conn:
                for r in result.itertuples():
                    sql = f"REPLACE INTO daily_price (open, high, low, close, volume, change_, mktcap, list_shrs, " \
                          f"code, name, date)" \
                          f"VALUES ({r.open}, {r.high}, {r.low}, {r.close}, {r.volume}, {r.change_}, " \
                          f"{r.mktcap}, {r.list_shrs}, '{r.code}', '{r.name}', '{r.date}')"
                    conn.execute(sql)
                print(f"[{date}] Update mktcap and list_shrs in daily price Successfully!")

    def get_date_range(self, st_date, end_date):
        """시작 날짜부터 종료 날짜 사이의 날짜를 하루씩 반환하는 함수
        Parameters
        ==========
        st_date: str, 시작 날짜 (ex) '20220101'
        end_date: str, 종료 날짜 (ex) '20220608'
        """
        for n in range(int((end_date - st_date).days) + 1):
            yield st_date + timedelta(days=n)

    def update_daily_price(self, start, end):
        """일정 기간 동안의 주식 시세를 업데이트
        Parameters
        ==========
        start: str, 시작 날짜 (ex) '2022-01-01'
        end: str, 종료 날짜 (ex) '2022-06-08'
        """
        stock = self.read_all_stock()
        for idx in range(len(stock)):
            code = stock['code'].values[idx]
            company = stock['company'].values[idx]
            self.replace_into_daily_price_db(code, company, start, end)
        print(f"Update daily price between [{start}] and [{end}] Successfully!")

    def getIncomeStatement(self, code, rpt_type, freq):
        """[FnGuide] 공시기업의 최근 4개 연간 및 4개 분기 손익계산서를 수집하는 함수
        Parameters
        ==========
        code: str, 종목코드
        rpt_type: str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq: str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        """
        items_en = [
            'rev', 'cgs', 'gross',
            'sga', 'sga1', 'sga2', 'sga3', 'sga4', 'sga5', 'sga6', 'sga7', 'sga8', 'opr', 'opr_',
            'fininc', 'fininc1', 'fininc2', 'fininc3', 'fininc4', 'fininc5',
            'fininc6', 'fininc7', 'fininc8', 'fininc9', 'fininc10', 'fininc11',
            'fincost', 'fincost1', 'fincost2', 'fincost3', 'fincost4', 'fincost5',
            'fincost6', 'fincost7', 'fincost8', 'fincost9', 'fincost10',
            'otherrev', 'otherrev1', 'otherrev2', 'otherrev3', 'otherrev4', 'otherrev5', 'otherrev6', 'otherrev7',
            'otherrev8',
            'otherrev9', 'otherrev10', 'otherrev11', 'otherrev12', 'otherrev13', 'otherrev14', 'otherrev15',
            'otherrev16',
            'othercost', 'othercost1', 'othercost2', 'othercost3', 'othercost4', 'othercost5',
            'othercost6', 'othercost7', 'othercost8', 'othercost9', 'othercost10', 'othercost11', 'othercost12',
            'otherpl', 'otherpl1', 'otherpl2', 'otherpl3', 'otherpl4',
            'ebit', 'tax', 'contop', 'discontop', 'netinc'
        ]

        if rpt_type.upper() == 'CONSOLIDATED':
            # 연결 연간 손익계산서(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D" \
                  "&NewMenuID=103&stkGb=701".format(code)
            items_en = items_en + ['netinc1', 'netinc2']

        else:
            # 별도 연간 손익계산서(ReportGB=B)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B" \
                  "&NewMenuID=103&stkGb=701".format(code)

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':  # 연간 손익계산서 영역 추출
            is_a = soup.find(id='divSonikY')
            num_col = 3  # 최근 3개 연간 데이터
        else:  # 분기 손익계산서 영역 추출 freq.upper() == 'Q'
            is_a = soup.find(id='divSonikQ')
            num_col = 4  # 최근 4개 분기 데이터

        if is_a is None:
            return None

        is_a = is_a.find_all(['tr'])
        items_kr = [is_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '')
                    .replace('계산에 참여한 계정 펼치기', '') for m in range(1, len(is_a))]
        period = [is_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        if len(items_en) != len(is_a) - 1:
            return None

        for item, i in zip(items_en, range(1, len(is_a))):
            temps = []
            for j in range(0, num_col):
                temp = [float(is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', ''))
                        if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != ''
                        else (0 if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0'
                        else 0)]
                temps.append(temp[0])
            globals()[item] = temps

        # 지배/비지배 항목 처리
        if rpt_type.upper() == 'CONSOLIDATED':  # 연결 손익계산서는 아무 것도 하지 않음
            pass
        else:  # 별도 손익계산서 해당 항목을 Null값으로 채움
            globals()['netinc1'], globals()['netinc2'] = [np.NaN] * num_col, [np.NaN] * num_col

        is_domestic = pd.DataFrame({'stock_code': code, 'period': period,
                                    'Revenue': globals()['rev'], 'Cost_of_Goods_Sold': globals()['cgs'],
                                    'Gross_Profit': globals()['gross'],
                                    'Sales_General_Administrative_Exp_Total': globals()['sga'],
                                    'Operating_Profit_Total': globals()['opr'],
                                    'Operating_Profit_Total_': globals()['opr_'],
                                    'Financial_Income_Total': globals()['fininc'],
                                    'Financial_Costs_Total': globals()['fincost'],
                                    'Other_Income_Total': globals()['otherrev'],
                                    'Other_Costs_Total': globals()['othercost'],
                                    'Subsidiaries_JointVentures_PL_Total': globals()['otherpl'],
                                    'EBIT': globals()['ebit'], 'Income_Taxes_Exp': globals()['tax'],
                                    'Profit_Cont_Operation': globals()['contop'],
                                    'Profit_Discont_Operation': globals()['discontop'],
                                    'Net_Income_Total': globals()['netinc'],
                                    'Net_Income_Controlling': globals()['netinc1'],
                                    'Net_Income_Noncontrolling': globals()['netinc2']})
        is_domestic = is_domestic.drop(columns=['Operating_Profit_Total_'])
        is_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        is_domestic.fillna('null', inplace=True)

        return is_domestic

    def replace_into_krx_income_statement_db(self, IS):
        """손익계산서 DB에 업데이트"""
        try:
            with self.engine.connect() as conn:
                for r in IS.itertuples():
                    sql = f"REPLACE INTO krx_income_statement VALUES " \
                          f"('{r.stock_code}', '{r.period}', {r.Revenue}, {r.Cost_of_Goods_Sold}, {r.Gross_Profit}," \
                          f"{r.Sales_General_Administrative_Exp_Total}, {r.Operating_Profit_Total}, {r.Financial_Income_Total}," \
                          f"{r.Financial_Costs_Total}, {r.Other_Income_Total}, {r.Other_Costs_Total}, " \
                          f"{r.Subsidiaries_JointVentures_PL_Total}, {r.EBIT}, {r.Income_Taxes_Exp}," \
                          f"{r.Profit_Cont_Operation}, {r.Profit_Discont_Operation}, {r.Net_Income_Total}," \
                          f"{r.Net_Income_Controlling}, {r.Net_Income_Noncontrolling}, '{r.rpt_type}')"
                    conn.execute(sql)
                    print(f"[#{r.stock_code}] Update [{r.rpt_type}] Income Statement [{r.period}] Successfully!")
        except Exception as e:
            print("Exception occured : ", str(e))
            return None

    def update_income_statement(self):
        """모든 종목의 손익계산서 업데이트"""
        stock = self.read_all_stock()
        for idx in range(len(stock)):
            code = stock['code'].values[idx]
            IS1 = self.getIncomeStatement(code, 'Consolidated', 'A')  # 연결 연간
            if IS1 is not None:
                self.replace_into_krx_income_statement_db(IS1)
            IS2 = self.getIncomeStatement(code, 'Unconsolidated', 'A')  # 별도 연간
            if IS2 is not None:
                self.replace_into_krx_income_statement_db(IS2)
            IS3 = self.getIncomeStatement(code, 'Consolidated', 'Q')  # 연결 분기
            if IS3 is not None:
                self.replace_into_krx_income_statement_db(IS3)
            IS4 = self.getIncomeStatement(code, 'Unconsolidated', 'Q')  # 별도 분기
            if IS4 is not None:
                self.replace_into_krx_income_statement_db(IS4)

    def getBalanceSheet(self, code, rpt_type, freq):
        """[FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 재무상태표를 수집하는 함수

        Parameters
        ==========
        code: str, 종목코드
        rpt_type: str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq: str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        """

        items_en = [
            'assets', 'curassets', 'curassets1', 'curassets2', 'curassets3', 'curassets4', 'curassets5',
            'curassets6',
            'curassets7', 'curassets8', 'curassets9', 'curassets10', 'curassets11',
            'ltassets', 'ltassets1', 'ltassets2', 'ltassets3', 'ltassets4', 'ltassets5', 'ltassets6', 'ltassets7',
            'ltassets8', 'ltassets9', 'ltassets10', 'ltassets11', 'ltassets12', 'ltassets13', 'finassets',
            'liab', 'curliab', 'curliab1', 'curliab2', 'curliab3', 'curliab4', 'curliab5',
            'curliab6', 'curliab7', 'curliab8', 'curliab9', 'curliab10', 'curliab11', 'curliab12', 'curliab13',
            'ltliab', 'ltliab1', 'ltliab2', 'ltliab3', 'ltliab4', 'ltliab5', 'ltliab6',
            'ltliab7', 'ltliab8', 'ltliab9', 'ltliab10', 'ltliab11', 'ltliab12', 'finliab',
            'equity', 'equity1', 'equity2', 'equity3', 'equity4', 'equity5', 'equity6', 'equity7', 'equity8'
        ]

        if rpt_type.upper() == 'CONSOLIDATED':
            # 연결 연간 재무상태표(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D" \
                  "&NewMenuID=103&stkGb=701".format(code)

        else:
            # 별도 연간 재무상태표(ReportGB=B)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B" \
                  "&NewMenuID=103&stkGb=701".format(code)
            items_en = [item for item in items_en if item not in ['equity1', 'equity8']]

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':  # 연간 재무상태표 영역 추출
            bs_a = soup.find(id='divDaechaY')
            num_col = 3  # 최근 3개 연간 데이터
        else:  # 분기 재무상태표 영역 추출 freq.upper() == 'Q'
            bs_a = soup.find(id='divDaechaQ')
            num_col = 4  # 최근 4개 분기 데이터

        if bs_a is None:
            return None
        bs_a = bs_a.find_all(['tr'])

        items_kr = [
            bs_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
            for m in range(1, len(bs_a))]
        period = [bs_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        if len(items_en) != len(bs_a) - 1:
            return None

        for item, i in zip(items_en, range(1, len(bs_a))):
            temps = []
            for j in range(0, num_col):
                temp = [float(bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', ''))
                        if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != ''
                        else (0 if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0'
                        else 0)]
                temps.append(temp[0])
            globals()[item] = temps

        # 지배/비지배 항목 처리
        if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간 재무상태표는 아무 것도 하지 않음
            pass
        else:  # 별도 연간 재무상태표 해당 항목을 Null값으로 채움
            globals()['equity1'], globals()['equity8'] = [np.NaN] * num_col, [np.NaN] * num_col

        bs_domestic = pd.DataFrame({'stock_code': code, 'period': period,
                                    'Assets_Total': globals()['assets'], 'Current_Assets_Total': globals()['curassets'],
                                    'LT_Assets_Total': globals()['ltassets'], 'Other_Fin_Assets': globals()['finassets'],
                                    'Liabilities_Total': globals()['liab'], 'Current_Liab_Total': globals()['curliab'],
                                    'LT_Liab_Total': globals()['ltliab'], 'Other_Fin_Liab_Total': globals()['finliab'],
                                    'Equity_Total': globals()['equity'], 'Paid_In_Capital': globals()['equity2'],
                                    'Contingent_Convertible_Bonds': globals()['equity3'],
                                    'Capital_Surplus': globals()['equity4'], 'Other_Equity': globals()['equity5'],
                                    'Accum_Other_Comprehensive_Income': globals()['equity6'],
                                    'Retained_Earnings': globals()['equity7']
                                    })
        bs_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        bs_domestic.fillna('null', inplace=True)

        return bs_domestic

    def replace_into_krx_balance_sheet_db(self, BS):
        """재무상태표 DB에 업데이트"""
        try:
            with self.engine.connect() as conn:
                for r in BS.itertuples():
                    sql = f"REPLACE INTO krx_balance_sheet VALUES " \
                          f"('{r.stock_code}', '{r.period}', {r.Assets_Total}, {r.Current_Assets_Total}," \
                          f"{r.LT_Assets_Total}, {r.Other_Fin_Assets}, {r.Liabilities_Total}, {r.Current_Liab_Total}," \
                          f"{r.LT_Liab_Total}, {r.Other_Fin_Liab_Total}, {r.Equity_Total}," \
                          f"{r.Paid_In_Capital}, {r.Contingent_Convertible_Bonds}, {r.Capital_Surplus}, {r.Other_Equity}," \
                          f"{r.Accum_Other_Comprehensive_Income}, {r.Retained_Earnings}, '{r.rpt_type}' )"
                    conn.execute(sql)
                    print(f"[#{r.stock_code}] Update [{r.rpt_type}] Balance Sheet [{r.period}] Successfully!")
        except Exception as e:
            print("Exception occured : ", str(e))
            return None

    def update_balance_sheet(self):
        """모든 종목의 재무상태표 업데이트"""
        stock = self.read_all_stock()
        for idx in range(len(stock)):
            code = stock['code'].values[idx]
            BS1 = self.getBalanceSheet(code, 'Consolidated', 'A')  # 연결 연간
            if BS1 is not None:
                self.replace_into_krx_balance_sheet_db(BS1)
            BS2 = self.getBalanceSheet(code, 'Unconsolidated', 'A')  # 별도 연간
            if BS2 is not None:
                self.replace_into_krx_balance_sheet_db(BS2)
            BS3 = self.getBalanceSheet(code, 'Consolidated', 'Q')  # 연결 분기
            if BS3 is not None:
                self.replace_into_krx_balance_sheet_db(BS3)
            BS4 = self.getBalanceSheet(code, 'Unconsolidated', 'Q')  # 별도 분기
            if BS4 is not None:
                self.replace_into_krx_balance_sheet_db(BS4)

    def getCashFlow(self, code, rpt_type, freq):
        """[FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 현금흐름표를 수집하는 함수

        Parameters
        ==========
        code: str, 종목코드
        rpt_type: str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq: str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        """

        items_en = [
            'cfo', 'cfo1', 'cfo2', 'cfo3', 'cfo4', 'cfo5', 'cfo6', 'cfo7',
            'cfi', 'cfi1', 'cfi2', 'cfi3', 'cff', 'cff1', 'cff2', 'cff3',
            'cff4', 'cff5', 'cff6', 'cff7', 'cff8', 'cff9'
        ]

        if rpt_type.upper() == 'CONSOLIDATED':
            # 연결 연간 현금흐름표(ReportGB=D)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D" \
                  "&NewMenuID=103&stkGb=701".format(code)

        else:
            # 별도 연간 현금흐름표(ReportGB=B)
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B" \
                  "&NewMenuID=103&stkGb=701".format(code)
            items_en = [item for item in items_en if item not in ['equity1', 'equity8']]

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':  # 연간 현금흐름표 영역 추출
            cf_a = soup.find(id='divCashY')
            num_col = 3  # 최근 3개 연간 데이터
        else:  # 분기 현금흐름표 영역 추출 freq.upper() == 'Q'
            cf_a = soup.find(id='divCashQ')
            num_col = 4  # 최근 4개 분기 데이터

        if cf_a is None:
            return None
        cf_a = cf_a.find_all(['tr'])

        items_kr = [cf_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                    for m in range(1, len(cf_a))]
        period = [cf_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        # 수집할 인덱스 값 미리 설정함
        idx = [1, 2, 3, 4, 39, 70, 75, 76, 84, 85, 99, 113, 121, 122, 134, 145, 153, 154, 155, 156, 157, 158]
        if len(cf_a) - 1 != 158:
            return None
        for item, i in zip(items_en, idx):
            temps = []
            for j in range(0, num_col):
                temp = [float(cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', ''))
                        if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != ''
                        else (0 if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0'
                        else 0)]
                temps.append(temp[0])
            globals()[item] = temps

        cf_domestic = pd.DataFrame({'stock_code': code, 'period': period,
                                    'CFO_Total': globals()['cfo'], 'Net_Income_Total': globals()['cfo1'],
                                    'Cont_Biz_Before_Tax': globals()['cfo2'], 'Add_Exp_WO_CF_Out': globals()['cfo3'],
                                    'Ded_Rev_WO_CF_In': globals()['cfo4'], 'Chg_Working_Capital': globals()['cfo5'],
                                    'CFO': globals()['cfo6'], 'Other_CFO': globals()['cfo7'],
                                    'CFI_Total': globals()['cfi'], 'CFI_In': globals()['cfi1'],
                                    'CFI_Out': globals()['cfi2'], 'Other_CFI': globals()['cfi3'],
                                    'CFF_Total': globals()['cff'], 'CFF_In': globals()['cff1'],
                                    'CFF_Out': globals()['cff2'], 'Other_CFF': globals()['cff3'],
                                    'Other_CF': globals()['cff4'], 'Chg_CF_Consolidation': globals()['cff5'],
                                    'Forex_Effect': globals()['cff6'],
                                    'Chg_Cash_and_Cash_Equivalents': globals()['cff7'],
                                    'Cash_and_Cash_Equivalents_Beg': globals()['cff8'],
                                    'Cash_and_Cash_Equivalents_End': globals()['cff9']
                                    })
        cf_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        cf_domestic.fillna('null', inplace=True)

        return cf_domestic

    def replace_into_krx_cash_flow_db(self, CF):
        """현금흐름표 DB에 업데이트"""
        try:
            with self.engine.connect() as conn:
                for r in CF.itertuples():
                    sql = f"REPLACE INTO krx_cash_flow VALUES " \
                          f"('{r.stock_code}', '{r.period}', {r.CFO_Total}, {r.Net_Income_Total}, {r.Cont_Biz_Before_Tax}," \
                          f"{r.Add_Exp_WO_CF_Out}, {r.Ded_Rev_WO_CF_In}, {r.Chg_Working_Capital}, {r.CFO}, {r.Other_CFO}," \
                          f"{r.CFI_Total}, {r.CFI_In}, {r.CFI_Out}, {r.Other_CFI}, {r.CFF_Total}, {r.CFF_In}, {r.CFF_Out}," \
                          f"{r.Other_CFF}, {r.Other_CF}, {r.Chg_CF_Consolidation}, {r.Forex_Effect}, " \
                          f"{r.Chg_Cash_and_Cash_Equivalents}, {r.Cash_and_Cash_Equivalents_Beg}, " \
                          f"{r.Cash_and_Cash_Equivalents_End}, '{r.rpt_type}')"
                    conn.execute(sql)
                    print(f"[#{r.stock_code}] Update [{r.rpt_type}] Cash Flow [{r.period}] Successfully!")
        except Exception as e:
            print("Exception occured : ", str(e))
            return None

    def update_cash_flow(self):
        """모든 종목의 현금흐름표 업데이트"""
        stock = self.read_all_stock()
        for idx in range(len(stock)):
            code = stock['code'].values[idx]
            CF1 = self.getCashFlow(code, 'Consolidated', 'A')  # 연결 연간
            if CF1 is not None:
                self.replace_into_krx_cash_flow_db(CF1)
            CF2 = self.getCashFlow(code, 'Unconsolidated', 'A')  # 별도 연간
            if CF2 is not None:
                self.replace_into_krx_cash_flow_db(CF2)
            CF3 = self.getCashFlow(code, 'Consolidated', 'Q')  # 연결 분기
            if CF3 is not None:
                self.replace_into_krx_cash_flow_db(CF3)
            CF4 = self.getCashFlow(code, 'Unconsolidated', 'Q')  # 별도 분기
            if CF4 is not None:
                self.replace_into_krx_cash_flow_db(CF4)
