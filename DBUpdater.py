from sqlalchemy import create_engine
import json
import pandas as pd
from datetime import datetime


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
        with self.engine.connect() as connection:
            sql = "SELECT max(last_update) FROM company_info"
            rs = connection.execute(sql).first()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last_update)" \
                          f"VALUES ('{code}', '{company}', '{today}')"
                    connection.execute(sql)
                    self.codes[code] = company
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f"[{tmnow}] Update {len(krx)} company info Successfully! ")
            else:
                print("Already updated today.")


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.update_comp_info()
