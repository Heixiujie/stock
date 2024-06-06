# -*- coding:utf-8 -*-
from mushare.mushare import MuShare
import pandas as pd
import time
import random

if __name__ == "__main__":
    title = ['symbol', 'code', 'name', 'trade', 'pricechange', 'changepercent',
       'buy', 'sell', 'settlement', 'open', 'high', 'low', 'volume', 'amount',
       'ticktime', 'per', 'pb', 'mktcap', 'nmc', 'turnoverratio', 'block']
    data = pd.DataFrame(columns=title)
    ms = MuShare()
    i = 0
    for code, name in ms.stk_sw3_blocks().items():
        block_detail = ms.stk_sw3_detail(block_code=code)
        df = pd.DataFrame(block_detail)
        df['block'] = name
        data = pd.concat([data, df])
        time.sleep(random.randint(1, 3))
    data.set_index(data['symbol'], inplace=True)
    data = data.astype({'code': 'object'})
    data.to_csv('../data/sw2.csv', encoding='gbk')