# -*- coding: UTF-8 -*-

import time
import re
import json

import requests
import pandas as pd
import numpy as np
import tqdm
import scrapy
import yfinance as yf
#import h5py
#import pyodbc
from scraper.items import YahooPriceItem
from scrapy_redis.spiders import RedisSpider

class YahooPriceSpider(RedisSpider):
#class YahooPriceSpider(scrapy.Spider):
    name = 'yahoo_price'
    redis_key = 'yahoopricespider:start_urls'

    def start_requests(self):
        if isinstance(self, RedisSpider):
            return
        url = 'https://quality.data.gov.tw/dq_download_json.php?nid=11549&md5_url=bb878d47ffbe7b83bfc1b41d0b24946e'
        yield scrapy.Request(url, callback = self.parse)

    def parse(self, response):
        stock_list = pd.DataFrame(response.json())
        historical_data = pd.DataFrame()

        for i in tqdm.tqdm(stock_list.index[0:1]):    
            # 抓取股票資料
            stock_id = stock_list.loc[i, '證券代號'] + '.TW'
            data = yf.Ticker(stock_id)
            #df = data.history(period="max") #無adj price
            df = yf.download(stock_id)
            df.reset_index(level=0, inplace=True)
            # 增加股票代號
            df['ID'] = stock_list.loc[i, '證券代號']

            # 合併
            historical_data = pd.concat([historical_data, df])
            time.sleep(0.8)
        
        item = YahooPriceItem()
        item['ID'] = historical_data['ID'].tolist()
        item['Date'] = historical_data['Date'].astype(str).tolist()
        item['Open'] = historical_data['Open'].tolist()
        item['High'] = historical_data['High'].tolist()
        item['Low'] = historical_data['Low'].tolist()
        item['Close'] = historical_data['Close'].tolist()
        item['AdjClose'] = historical_data['Adj Close'].tolist()
        item['Volume'] = historical_data['Volume'].tolist()

        return item



