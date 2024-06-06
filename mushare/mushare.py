# -*- coding:utf-8 -*-

import json
import time
import pandas as pd
import random
import requests



class MuShare:

    def __init__(self):
        pass

    def _geturl(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
        }
        try:
            r = requests.get(url, timeout=10, headers=headers)
            print("Retrive url {} successful.".format(url))
        except Exception as e:
            print("Retrive url {} failed.".format(url))
            r = None
        return r

    def stk_sw3_blocks(self):
        _sw3_blocks = {}
        _url = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodes'
        _r = self._geturl(_url)
        if json.loads(_r.text) and _r is not None:
            _data = json.loads(_r.text)
            for i in _data[1][0][1]:
                if '申万三级' in i:
                    for j in i[1]:
                        _sw3_blocks[j[2]] = j[0]
        return _sw3_blocks

    def stk_sw3_detail(self, block_code):
        _url = ('https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?'
               'page=1&num=1000&node=')
        r = self._geturl("{}{}".format(_url, block_code))
        if r is not None:
            _data = json.loads(r.text)
        else:
            _data = None
        return _data


if __name__ == "__main__":
    pass


