# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scraper import settings
from scraper.items import DividendItem, YahooPriceItem
import MySQLdb


class DividendPipeline:
    def __init__(self):
        self.connect = MySQLdb.connect(
            host=settings.MYSQL_HOST,
            db=settings.MYSQL_DB,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASS,
            charset='utf8', use_unicode=True)
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        if type(item).__name__ == 'DividendItem':
            try:
                for i in range(len(item['ID'])):
                    exist_data = self.cursor.execute('''select * from dividend where ID = %s and YQ = %s''', (str(item['ID'][i]),str(item['YQ'][i])))
                    if not exist_data :
                        self.cursor.execute('''insert into dividend ( \
                            ID,Name,YQ,BaseDate,StockDiv,StockDivDate,CashDiv,CashDivDate,CashDivGetDate,TotalAddShare,AddShareRate,AddSharePrice,AnncDate,AnncTime,PerShare) value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ''',
                            (str(item['ID'][i]), 
                             str(item['Name'][i]),
                             item['YQ'][i],
                             item['BaseDate'][i],
                             item['StockDiv'][i],
                             item['StockDivDate'][i],
                             item['CashDiv'][i],
                             item['CashDivDate'][i],
                             item['CashDivGetDate'][i],
                             item['TotalAddShare'][i],
                             item['AddShareRate'][i],
                             item['AddSharePrice'][i],
                             item['AnncDate'][i],
                             item['AnncTime'][i],
                             item['PerShare'][i],
                             ))
                    self.connect.commit()
                print('------successfully insert to DB------')
            except Exception as error:
                print('----- ERROR in inserting DB -----')
                self.connect.rollback()
                print(error)


        return item


class YahooPricePipeline:
    def __init__(self):
        self.connect = MySQLdb.connect(
            host=settings.MYSQL_HOST,
            db=settings.MYSQL_DB,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASS,
            charset='utf8', use_unicode=True)
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        if type(item).__name__ == 'YahooPriceItem':
            try:
                for i in range(len(item['ID'])):
                    exist_data = self.cursor.execute('''select * from yahoo_price where ID = %s and Date = %s''', (item['ID'][i],item['Date'][i]))
                    if not exist_data :
                        self.cursor.execute('''insert into yahoo_price ( \
                            ID,Date,Open,High,Low,Close,AdjClose,Volume) value (%s, %s, %s, %s, %s, %s, %s, %s) ''',
                            (str(item['ID'][i]),
                             item['Date'][i],
                             item['Open'][i],
                             item['High'][i],
                             item['Low'][i],
                             item['Close'][i],
                             item['AdjClose'][i],
                             item['Volume'][i],
                             ))
                    self.connect.commit()
                print('------successfully insert to DB:yahoo_price ------')
            except Exception as error:
                print('----- ERROR in inserting DB:yahoo_price -----')
                self.connect.rollback()
                print(error)

        return item









