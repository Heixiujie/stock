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


def mysql_sql_to_df(sql_text):
    try:
        conn = xgz.conn_mysql(host='10.66.249.235',user='stock_user',password='stock_password',db='stock')
        sql_text = sql_text
        df = pd.read_sql(sql=sql_text,con=conn)
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

    try:
        symbol = re.search(r'(\d+).(SZ|SH|BJ)', ts_code).group(2).lower() + re.search(r'(\d+).(SZ|SH|BJ)',
                                                                                      ts_code).group(1)
        sql_text = 'select * from tbl_stk_kline_data where ts_code=\'{}\' order by trade_date desc limit 50;'.format(ts_code)
        delta_day = 3
        delta_day_bef = delta_day + 1
        delta_day_aft = delta_day - 1
        df4 = mysql_sql_to_df(sql_text=sql_text)
        df2 = df4[delta_day_bef:]
        # print(df3.loc[0,'pct_chg'])
        if df2.size > 20:
            day0_str, day1_str, day2_str, day3_str, day4_str = df2.index[0:5]
            print("Analyze {} ...".format(ts_code))
            if (df2.size > 20
                    # and len(df2[0:3][ df2[0:3]['low'] < df2[0:3]['LB'] ]) >= 1
                    # and df2.at[day0_str, 'low'] == df2[0:20]['low'].min()
                    # and 1 <= df4.loc[delta_day, 'pct_chg'] <= 4
                    # and (df2[0:20]['close'].max() - df2[0:20]['close'].min()) / df2[0:20]['close'].max() > 0.15
                    # and df4.loc[delta_day, 'open'] <= df2.at[day0_str, 'close'] < df2.at[day0_str, 'open'] < df4.loc[delta_day, 'close']
                    # and df4.loc[delta_day, 'close'] > df2.at[day0_str, 'open']
                    # and df4.loc[delta_day, 'close'] > df4.loc[delta_day, 'open']
                    # and df2.at[day0_str, 'close'] < df2.at[day0_str, 'open']

                    and df4.loc[delta_day, 'vol'] < df2[0:20]['vol'].mean()
                    # and abs(df2.at[day0_str, 'pct_chg']) < -2
            ):
                # sina_url = 'https://finance.sina.com.cn/realstock/company/' + symbol + '/nc.shtml'
                msg = '{:<10s} {:<20s} {:>.2f} {:>.2f} {:>.2f} {:>.2f} {:>.2f} {:>.2f} {:>.2f}\n'.format(
                    symbol,df4.loc[delta_day_aft,'name'],
                    df4.loc[delta_day, 'close'],
                    df4.loc[delta_day, 'pct_chg'],
                    df4.loc[delta_day_aft,'close'],
                    df4.loc[delta_day_aft, 'pct_chg'],
                    df4.loc[delta_day_aft,'open'],
                    df4.loc[delta_day_aft,'high'],
                    df4.loc[delta_day_aft,'low']
                     )
                print(msg)
                with open(filename, 'a+') as f:
                        f.write(msg)
    except Exception as e:
        print("Check {} failed.".format(ts_code))
        print(e)

def main(buy_date):
    df = mysql_tbl_to_df('tbl_stk_basics', index='symbol')
    filename = os.path.join(root_dir_path, '../report/dailybbb' + buy_date)
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

