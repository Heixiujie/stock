# -*- coding:utf-8 -*-
import re

from bs4 import BeautifulSoup as bs
import requests
import json
from stock_tech.xgz import Xgz
import pandas as pd

headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
            }
url = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodes'
# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj?page=1&num=200000&sort=r0_ratio&asc=0&bankuai=&shichang='
r = requests.get(url, headers=headers)
data = json.loads(r.text)
for i in data[1][0][1]:
    if '申万三级' in i:
        print(i)
        for j in i[1]:
            print('{:30s}{:10s}'.format(j[0], j[2]))
