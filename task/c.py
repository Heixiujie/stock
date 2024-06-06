from stock_tech.xgz import Xgz, MyThread
import re
import pandas as pd
import time
from datetime import datetime,timedelta
import os
import threading
from stock_tech.xgz import Xgz

scr_abs_path = os.path.abspath(__file__)
root_dir_path = os.path.split(scr_abs_path)[0]


xgz = Xgz()


def mysql_sql_to_df(sql_text,index):
    try:
        conn = xgz.conn_mysql(host='10.66.249.235',user='stock_user',password='stock_password',db='stock')
        sql_text = sql_text
        df = pd.read_sql(sql=sql_text,index_col=index,con=conn)
    except Exception as e:
        print(e)
    else:
        return df[:500]


def mysql_tbl_to_df(table_name,index):
    try:
        conn = xgz.conn_mysql(host='10.66.249.235',user='stock_user',password='stock_password',db='stock')
        df = pd.read_sql_table(table_name=table_name,index_col=index,con=conn)
    except Exception as e:
        print(e)
    else:
        return df


def check(ts_code, filename):
    # ts_code = '000595.SZ'
    # print("Analyze {} ...".format(ts_code))
    try:
        symbol = re.search(r'(\d+).(SZ|SH|BJ)', ts_code).group(2).lower() + re.search(r'(\d+).(SZ|SH|BJ)',
                                                                                      ts_code).group(1)
        sql_text = 'select * from tbl_stk_kline_data where ts_code=\'{}\' order by trade_date desc limit 50;'.format(ts_code)
        df2 = mysql_sql_to_df(sql_text=sql_text, index='trade_date')
        # df2 = df4[0:]
        # print(df3.loc[0,'pct_chg'])
        if df2.size > 20:
            day0_str, day1_str, day2_str, day3_str, day4_str = df2.index[0:5]
            if (df2.size > 20
                    # and df2.at[day0_str,'pct_chg'] < -2
                    # and df2.at[day0_str, 'vol'] > df2[0:20]['vol'].mean() * 1.5
                    and len(df2[0:3][ df2[0:3]['low'] < df2[0:3]['LB'] ]) >= 1
                    and df2[0:1]['low'].min() == df2[0:20]['low'].min()
                    # and len(df2[0:10][ df2[0:10]['low'] <= df2[0:10]['LB'] ]) > 0
                    and (df2[0:20]['close'].max() - df2[0:20]['close'].min()) / df2[0:20]['close'].max() > 0.15
            ):
                # print("Got {}:".format(symbol))
                df3 = xgz.stk_realtime_quotes(symbol=symbol)
                df4 = xgz.stk_cap_amount(symbol=symbol)
                if (1 == 1
                        and float(df3['pct_chg']) > 1
                        # and float(df3['price']) > df2.at[day0_str, 'open']
                        # and float(df3['open']) <= df2.at[day0_str, 'close']
                        # and df3['vol'] > df2[0:20]['vol'].mean() * 1.5
                ):
                    sina_url = 'https://finance.sina.com.cn/realstock/company/' + symbol + '/nc.shtml'
                    msg = '{}|{}|{}|{}|{}|{}|{}|{}\n'.format(symbol, df3['name'],
                                                    df3['price'], df3['pct_chg'], df2.at[day0_str, 'pct_chg'],
                                                     df3['turnover'], df3['circ_value'], round(float(df4['netamount']),0))
                    print(msg)
                    with open(filename, 'a+') as f:
                        f.write(msg)
    except Exception as e:
        print("Check {} failed.".format(ts_code))
        print(e)

def main(buy_date):
    df = mysql_tbl_to_df('tbl_stk_basics', index='symbol')
    filename = os.path.join(root_dir_path, '../report/dailyccc' + buy_date)
    print(filename)
    if os.path.exists(filename):
        os.remove(filename)
    for ts_code in df['ts_code']:
        if re.search(r'^3.*|^68.*|^43.*|^8.*|^.*BJ', ts_code):
            continue
        # print(ts_code)
        if threading.activeCount() > 50:
            time.sleep(0.1)
        else:
            t = threading.Thread(target=check, args=(ts_code, filename,))
            t.start()
    if threading.activeCount() > 0:
        time.sleep(1)




if __name__ == '__main__':
    delta = timedelta(days=0)
    end_date = (datetime.today() - delta).strftime('%Y-%m-%d')
    print(end_date)
    main(buy_date=end_date)

