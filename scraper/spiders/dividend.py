# -*- coding: UTF-8 -*-

import requests
import pandas as pd
import time
import tqdm
import scrapy
import json
import re
from scraper.items import DividendItem

class DividendSpider(scrapy.Spider):
    name = 'dividend'


    def start_requests(self):
#        if isinstance(self, RedisSpider):
#            return
        for y in range(99,109):
            form_data = {
                'encodeURIComponent':'1',
                'step':'1',
                'firstin':'1',
                'off':'1',
                'TYPEK':"sii",
                'co_id_1': '1101',
                'co_id_2': '9999',
                'year': str(y)
            }
            url = "https://mops.twse.com.tw/mops/web/ajax_t108sb27"
            yield scrapy.FormRequest(url, formdata=form_data)

    def parse(self, response):
        try:
            html_df = pd.read_html(response.text)[0]
        except Exception as error:
            print(error)
            return
        html_df.columns = html_df.columns.get_level_values((len(html_df.columns[0])-1))
        html_df = html_df.rename(columns={'股利所屬期間':'股利所屬年度'})
        html_df = html_df[['公司代號', '公司名稱', '股利所屬年度', '權利分派基準日','盈餘轉增資配股(元/股)','除權交易日', '盈餘分配之股東現金股利(元/股)','除息交易日','現金股利發放日','現金增資總股數(股)','現金增資認股比率(%)','現金增資認購價(元/股)','公告日期', '公告時間', '普通股每股面額']]
        html_df = html_df[html_df['公司代號']!='公司代號']
        html_df = html_df.where(pd.notnull(html_df), None)
        item_col = ['ID','Name','YQ','BaseDate','StockDiv','StockDivDate','CashDiv','CashDivDate','CashDivGetDate','TotalAddShare','AddShareRate','AddSharePrice','AnncDate','AnncTime','PerShare']
        item = DividendItem()
        for i in range(len(item_col)):
            item[item_col[i]] = html_df[html_df.columns[i]].tolist()
            if item_col[i] in ['BaseDate','StockDivDate','CashDivDate','CashDivGetDate','AnncDate']:
                for j in range(len(item[item_col[i]])):
                    if isinstance(item[item_col[i]][j], str):
                        str_spl = item[item_col[i]][j].split('/')
                        str_spl[0] = str(int(str_spl[0]) + 1911)
                        item[item_col[i]][j] = '-'.join(str_spl)

            if item_col[i] == 'PerShare':
                for j in range(len(item[item_col[i]])):
                    try:
                        item[item_col[i]][j] = float(re.findall('\d+\.\d+',item[item_col[i]][j])[0])
                    except Exception as error:
                        item[item_col[i]][j] = None
        return item     
