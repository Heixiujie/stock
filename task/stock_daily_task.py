from stock_tech.xgz import Xgz
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import sys, os
print(sys.path)




scr_abs_path = os.path.abspath(__file__)
root_dir_path = os.path.abspath(scr_abs_path)

if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://stock_user:stock_password@127.0.0.1/stock?charset=utf8')
    xgz = Xgz()

    """数据表
    stk_lhb_broker_detail   龙虎榜股票营业部买入详细情况
    stk_k_line_data         股票K线数据
    """

    """
    获取股票基本信息
    """
    # print("\n ======Get Stock basic data...")
    # df = xgz.stk_basic_info()
    # # pd.io.sql.execute('truncate table stk_basics;', engine)
    # df.to_sql('tbl_stk_basics', engine, if_exists='replace', index=False)
    # print("\n ====== End ===============")

    # """
    # 获取股票龙虎榜机构买卖详细
    # """
    # for stk in set(ts.top_list()['code']):
    #     table = 'stk_lhb_broker_detail'
    #     sql_clear_broker_detail = 'delete from ' + table + ' where symbol = ' + stk + ' and date = \'' + dt + '\';'
    #     # print(sql_clear_broker_detail)
    #     print(stk)
    #     te.stk_clear_table(engine=engine, sql=sql_clear_broker_detail)
    #     try:
    #         df = te.stk_lhb_broker_detail(code=stk,date=dt)
    #         df.to_sql(table, engine, if_exists='append')
    #     except AttributeError as e:
    #         print(e)
    #
    # #
    #
    # """
    # 获取每日K线数据
    # """

    def stk_hist_data_loops(tscode):
        # tscode = '000890.SZ'
        enddate = datetime.strftime(datetime.today(),'%Y-%m-%d')
        df = xgz.stk_bar_info(ts_code=tscode, end_date=enddate)
        # print(df)
        try:
            df2 = df[['ts_code', 'close', 'open', 'low', 'high', 'vol', 'pct_chg',
                          'ma5', 'ma10', 'ma20', 'ma60', 'name', 'DIFF', 'DEA', 'MACD', 'BOLL', 'UB', 'LB', 'symbol',
                      'BBIBOLL','BBIUPER','BBIDOWN']]
            df2.to_sql('tbl_stk_kline_data',engine,if_exists='append')
            # return 'OK'
            print("{} K线入库成功。".format(tscode))
        except AttributeError as e:
            print("{} K线入库失败。".format(tscode))
            # return False

    df_stock_basic = pd.read_sql_table(table_name='tbl_stk_basics', index_col='ts_code', con=engine)
    sql = 'truncate table tbl_stk_kline_data;'
    pd.io.sql.execute(sql=sql, con=engine)
    print("\n ======Get Stock kline data...")
    # df_stock_basic = df_stock_basic[df_stock_basic['symbol'].str().contains('sh')]
    with ThreadPoolExecutor(max_workers=3) as excut:
        for ts_code in df_stock_basic.index:
            if re.search(r'.*\.SZ|.*\.SH', ts_code):
                excut.submit(stk_hist_data_loops, ts_code)
                # stk_hist_data_loops(ts_code)
            # if (threading.activeCount() > 5):
            #     time.sleep(0.2)
            # else:
            #     t = threading.Thread(target=stk_hist_data_loops, args=(ts_code,))
            #     t.start()

    print("\n ====== End ===============")


    print("\n ======获取股票票主力资金数据...")
    df = xgz.stk_zlzj_data()
    last_td = xgz.last_tddate()
    df['date'] = last_td
    sql = 'delete from tbl_stk_zlzj_data where date=\'{}\''.format(last_td)
    pd.io.sql.execute(sql=sql,con=engine)
    # df.set_index('date', inplace=True)
    df.to_sql('tbl_stk_zlzj_data',con=engine,if_exists='append',index=False)
    print("\n ====== End ===============")
    #
    # """
    # 获取股票前三交易价位信息
    # """
    # for stk in ts.get_stock_basics().index:
    #     print("获取交易价位：" + stk)
    #
    #     ticks = te.stk_get_hist_ticks(code=stk, date=dt)
    #     te.stk_clear_table(engine=engine,
    #                        sql='delete from stk_hist_ticks where code=' + '\'' + stk + '\'' + ' and date =' + '\'' + dt + '\';')
    #     try:
    #         df = pd.DataFrame(ticks)
    #         df.to_sql('stk_hist_ticks', engine, if_exists='append')
    #     except Exception as e:
    #         print(e)
