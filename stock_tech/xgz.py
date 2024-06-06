from stock_tech import cons as ct
import pandas as pd
import requests
import json
import re
import time
from datetime import datetime, timedelta
import threading
import numpy as np
from sqlalchemy import create_engine
import random
from dateutil.parser import parse
from tushare.util.formula import MACD, BOLL, BBI, MA, STD
import tushare as ts


class MyThread(threading.Thread):

    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None




class Xgz:

    def __init__(self, token='3f4e343cde9ad2a2566578abab884e2e2baba5b9609cc75e77481332'):
        self.token = token

    def xgz_connect(self):
        ts.set_token(self.token)
        pro = ts.pro_api(timeout=30)
        return pro

    def stk_zlzj_data(self):
        try:
            url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj?page=1&num=200000&sort=r0_ratio&asc=0&bankuai=&shichang='
            # requests.adapters.DEFAULT_RETRIES = 5
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
            }
            r = requests.get(url,headers=headers)
            print(r.text)
            txt = json.loads(r.text)
            df = pd.DataFrame(txt)
            # df.set_index('symbol', inplace=True)
            for index in df.index:
                ts_code = re.search(r'\w\w(.*)', df.at[index, 'symbol']).group(1) + '.' + \
                          re.search(r'(\w\w).*', df.at[index, 'symbol']).group(1).upper()
                # symbol = re.search(r'(\w\w).*', index).group(1).lower() + re.search(r'\w\w(.*)', index).group(1)
                df.at[index, 'ts_code'] = ts_code
            df = df.astype({'netamount': float, 'ratioamount': float, 'r0_ratio': float, 'turnover': float})
            r.close()
        except Exception as e:
            print(e)
        else:
            return df

    def stk_realtime_quotes(self, symbol):
        self.symbol = symbol
        real_quotes_url = 'http://qt.gtimg.cn/q={}'.format(symbol)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
        }
        r = requests.get(real_quotes_url, headers=headers)
        p = r.text.split(r'~')
        real_quotes = {
            'name': p[1],
            'price': p[3],
            'open': p[5],
            'high': p[41],
            'low': p[42],
            'vol': int(p[36]) * 100,
            'turnover': p[38],
            'circ_value': p[44],
            'pct_chg': p[32]
        }
        return real_quotes

    def stk_basic_info(self):
        tspro = self.xgz_connect()
        stk_base_info = tspro.stock_basic(list_status='L', fields='ts_code,symbol,name,industry,list_status')
        for index in stk_base_info.index:
            symbol = re.search(r'(\d+)\.(.*)', stk_base_info.at[index,'ts_code']).group(2).lower() + \
                        re.search(r'(\d+)\.(.*)', stk_base_info.at[index,'ts_code']).group(1)
            stk_base_info.at[index,'symbol'] = symbol
            # stk_base_info.set_index('symbol', inplace=True)
        return stk_base_info

    def BBIBOLL(self, DF, N1, N2, N3, N4, N, M):  # 多空布林线
        bbiboll = BBI(DF, N1, N2, N3, N4)
        UPER = bbiboll + M * STD(bbiboll, N)
        DOWN = bbiboll - M * STD(bbiboll, N)
        DICT = {'BBIBOLL': bbiboll.BBI, 'BBIUPER': UPER.BBI, 'BBIDOWN': DOWN.BBI}
        VAR = pd.DataFrame(DICT, index=bbiboll.index)
        return VAR

    def stk_bar_info(self, ts_code, end_date):

        engine = create_engine('mysql+pymysql://stock_user:stock_password@10.66.249.235/stock?charset=utf8',
                               encoding='utf-8')
        df_basic = pd.read_sql_table(table_name='tbl_stk_basics', index_col='ts_code', con=engine)
        delta = timedelta(days=365)
        start_date = datetime.today() - delta
        conn_pro = self.xgz_connect()
        df = pd.DataFrame()
        try:
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date.strftime('%Y-%m-%d'),
                            end_date=end_date, ma=[5, 10, 20, 60])
            # print(df.columns)
            df.set_index('trade_date', inplace=True)
            df.sort_index(inplace=True)
            df2 = MACD(df['close'], FAST=5, SLOW=10, MID=7)
            df3 = BOLL(df[['close']], N=20)
            df4 = self.BBIBOLL(df, N1=3, N2=6, N3=12, N4=24,M=6,N=11)
            df = df.join(df2)
            df = df.join(df3)
            df = df.join(df4)
            ts_symbol = re.search(r'(\d+).(SZ|SH|BJ)', ts_code).group(2).lower() + re.search(r'(\d+).(SZ|SH|BJ)',ts_code).group(1)
            ts_name = df_basic.loc[df_basic['symbol'] == ts_symbol]['name'].values[0]
            name_np = np.array([[ts_name] * len(df)][0])
            df['name'] = name_np
            df['symbol'] = np.array([[ts_symbol] * len(df)][0])
            df['trade_date'] = df.index
            return df
        except Exception as e:
            print('Get {} hist kline data failed.\n {}'.format(ts_code, e))

    def conn_mysql(self,host='10.66.249.235',user='stock_user',password='stock_password',db='stock'):
        # 初始化数据库连接，使用pymysql模块
        db_info = {'user': user,'password': password,
        'host': host,
        'port': 3306,
        'database': db}
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info,
            encoding='utf-8')
        return engine

    def trade_cal(self):
        conn_pro = self.xgz_connect()
        df = conn_pro.query('trade_cal', start_date='20180101')
        return df

    def is_holiday(self, date):
        '''
                判断是否为交易日，返回True or False
        '''
        df = self.trade_cal()
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y%m%d')
        else:
            date = date.strftime('%Y%m%d')
        index = df[df.cal_date == date].index[0]
        if df.at[index,'is_open'] == 0:
            return True
        else:
            return False


    def last_tddate(self):
        today = datetime.today().date()
        df_cal = self.trade_cal()
        if(self.is_holiday(today)):
            tddate = df_cal[df_cal.cal_date == today.strftime('%Y%m%d')].pretrade_date.values[0]
        else:
            tddate = today
        return str(tddate)


    def mysql_sql_to_df(self, sql_text, index):
        try:
            conn = self.conn_mysql()
            df = pd.read_sql(sql=sql_text, index_col=index, con=conn)
        except Exception as e:
            print(e)
        else:
            return df[:500]

    def mysql_tbl_to_df(self, table_name, index):
        try:
            conn = self.conn_mysql()
            df = pd.read_sql_table(table_name=table_name, index_col=index, con=conn)
        except Exception as e:
            print(e)
        else:
            return df

    def daily_price(self, trade_date):
        try:
            tspro = self.xgz_connect()
            df_daily = tspro.daily(trade_date=trade_date)
            df_daily.set_index('ts_code', inplace=True)
            return df_daily
        except Exception as e:
            print("Get stock daily price failed.")
            print(e)

    def stk_cap_amount(self, symbol):
        self.symbol = symbol
        url = 'http://vip.stock.finance.sina.com.cn/' \
              'quotes_service/api/json_v2.php/' \
              'MoneyFlow.ssi_ssfx_flzjtj?daima={}'.format(symbol)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
        }
        r = requests.get(url, headers=headers)
        data = json.loads(r.text)
        return data

if __name__ == "__main__":
    xgz = Xgz()
    # end_date = datetime.today().strftime('%Y%m%d')
    # conn_pro = xgz.xgz_connect()
    # enddate = datetime.strftime(datetime.today(), '%Y-%m-%d')
    # print(conn.get_today_all())
    # xgz.stk_daily_basic_data(ts_code='002937.SZ')
    # xgz.tddate_delta(date='20220108', n=1)

    # df = xgz.daily_price('20230629')
    df = xgz.stock_cap_amount('sz002965')
    print(df)

