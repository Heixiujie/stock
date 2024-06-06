from stock_tech import cons as ct
import pandas as pd
import requests
import json
import re
import time
import threading
from random import random

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib3 import urlopen, Request


def _get_type_data(url):
    try:
        request = Request(url)
        data_str = urlopen(request, timeout=10).read()
        data_str = data_str.decode('GBK')
        data_str = data_str.split('=')[1]
        data_json = json.loads(data_str)
        df = pd.DataFrame([[row.split(',')[0], row.split(',')[1]] for row in data_json.values()],
                          columns=['tag', 'name'])
        return df
    except Exception as er:
        print(str(er))


def get_concept_classified():
    ct._write_head()
    df = _get_type_data(ct.SINA_CONCEPTS_INDEX_URL % (ct.P_TYPE['http'],
                                                      ct.DOMAINS['sf'], ct.PAGES['cpt']))
    data = []
    for row in df.values:
        rowDf = _get_detail(row[0])
        if rowDf is not None:
            rowDf['c_name'] = row[1]
            print(rowDf)
            data.append(rowDf)
    if len(data) > 0:
        data = pd.concat(data, ignore_index=True)
    return data


def _get_detail(tag, retry_count=3, pause=random()):
    dfc = pd.DataFrame()
    p = 0
    num_limit = 100
    while (True):
        p = p + 1
        for _ in range(retry_count):
            time.sleep(pause)
            try:
                # ct._write_console()
                request = Request(ct.SINA_DATA_DETAIL_URL % (ct.P_TYPE['http'],
                                                             ct.DOMAINS['vsf'], ct.PAGES['jv'],
                                                             p, tag))
                request.add_header(key='User-Agent', val='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                         'Chrome/116.0.0.0 Safari/537.36')
                text = urlopen(request, timeout=10).read()
                text = text.decode('gbk')
                jstr = json.dumps(text)
                js = json.loads(jstr)
                df = pd.DataFrame(pd.read_json(js, dtype={'code': object}), columns=ct.THE_FIELDS)
                df = df[['code', 'name']]
                dfc = pd.concat([dfc, df])
                if df.shape[0] < num_limit:
                    return dfc
            except Exception:
                pass
            else:
                break
        # reg = re.compile(r'\,(.*?)\:')
        # text = reg.sub(r',"\1":', text)
        # text = text.replace('"{symbol', '{"symbol')
        # text = text.replace('{symbol', '{"symbol"')
        # text = text.replace('#', '')


def get_industry_classified(standard='sw'):
    """
        获取行业分类数据
    Parameters
    ----------
    standard
    sina:新浪行业 sw：申万 行业

    Returns
    -------
    DataFrame
        code :股票代码
        name :股票名称
        c_name :行业名称
    """
    if standard == 'sw':
        df = _get_type_data(ct.SINA_INDUSTRY_INDEX_URL % (ct.P_TYPE['http'],
                                                          ct.DOMAINS['vsf'], ct.PAGES['ids_sw']))
    else:
        # print(ct.SINA_INDUSTRY_INDEX_URL % (ct.P_TYPE['http'],
        #                                                   ct.DOMAINS['vsf'], ct.PAGES['ids']))
        df = _get_type_data(ct.SINA_INDUSTRY_INDEX_URL % (ct.P_TYPE['http'],
                                                          ct.DOMAINS['vsf'], ct.PAGES['ids']))
    data = []
    # ct._write_head()
    for row in df.values:
        print("{}...".format(row[1]))
        rowDf = _get_detail(row[0], retry_count=10)
        rowDf['c_name'] = row[1]
        data.append(rowDf)
    # print(data)
    if len(data) > 0:
        data = pd.concat(data, ignore_index=True)
        # df = pd.DataFrame(data)
    return data


def get_today_all():
    """
        一次性获取最近一个日交易日所有股票的交易数据
    return
    -------
      DataFrame
           属性：代码，名称，涨跌幅，现价，开盘价，最高价，最低价，最日收盘价，成交量，换手率，成交额，市盈率，市净率，总市值，流通市值
    """
    # ct._write_head()
    df = _parsing_dayprice_json('hs_a', 1)
    if df is not None:
        for i in range(2, ct.PAGE_NUM[1]):
            newdf = _parsing_dayprice_json('hs_a', i)
            time.sleep(random())
            if newdf.shape[0] > 0:
                df = pd.concat([df, newdf], ignore_index=True)
            else:
                break
    # df = pd.concat([_parsing_dayprice_json('shfxjs', 1), df],
    #                ignore_index=True)
    return df


def _parsing_dayprice_json(types=None, page=1):
    """
           处理当日行情分页数据，格式为json
     Parameters
     ------
        pageNum:页码
     return
     -------
        DataFrame 当日所有股票交易数据(DataFrame)
    """
    # ct._write_console()
    request = Request(ct.SINA_DAY_PRICE_URL % (ct.P_TYPE['http'], ct.DOMAINS['vsf'],
                                               ct.PAGES['jv'], types, page))
    request.add_header(key='User-Agent', val='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                             'AppleWebKit/537.36 (KHTML, like Gecko) '
                                             'Chrome/116.0.0.0 Safari/537.36')
    print("Processing market {} page {}...".format(types, page))
    text = urlopen(request, timeout=10).read()
    text = text.decode('gbk')
    if text == 'null':
        return None
    # reg = re.compile(r'\,(.*?)\:')
    # text = reg.sub(r',"\1":', text.decode('gbk') if ct.PY3 else text)
    # text = text.replace('"{symbol', '{"symbol')
    # text = text.replace('{symbol', '{"symbol"')
    # text = text.replace('""', '"')
    # text = text.replace('"{"', '{"')
    if ct.PY3:
        js = json.dumps(text)
    else:
        js = json.dumps(text, encoding='GBK')
    js = json.loads(js)
    df = pd.DataFrame(pd.read_json(js, dtype={'code': object}),
                      columns=ct.DAY_TRADING_COLUMNS)
    df = df.drop('symbol', axis=1)
    #     df = df.ix[df.volume > 0]
    return df


if __name__ == "__main__":
    # data = get_industry_classified(standard='sina')
    # print(data)
    df = get_today_all()
    print(df)
