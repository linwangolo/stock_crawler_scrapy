# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DividendItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ID = scrapy.Field()
    Name = scrapy.Field()
    YQ = scrapy.Field()
    BaseDate = scrapy.Field()
    StockDiv = scrapy.Field()
    StockDivDate = scrapy.Field()
    CashDiv = scrapy.Field()
    CashDivDate = scrapy.Field()
    CashDivGetDate = scrapy.Field()
    TotalAddShare = scrapy.Field()
    AddShareRate = scrapy.Field()
    AddSharePrice = scrapy.Field()
    AnncDate = scrapy.Field()
    AnncTime = scrapy.Field()
    PerShare = scrapy.Field()

class YahooPriceItem(scrapy.Item):
    ID = scrapy.Field()
    Date = scrapy.Field()
    Open = scrapy.Field()
    High = scrapy.Field()
    Low = scrapy.Field()
    Close = scrapy.Field()
    AdjClose = scrapy.Field()
    Volume = scrapy.Field()








   

