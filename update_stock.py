#!/usr/local/bin/python3
import os
import sys
sys.path.append(os.getcwd())

import mysql_cmd

import time
import enum
import pymysql
import datetime
import requests
import numpy as np
import pandas as pd
from io import StringIO

#{{{# Enum
class StockJuristicTableIndex(enum.IntEnum):
    BUY_VOLUME          = 0
    SOLD_VOLUME         = 1
    DIFF_VOLUME         = 2

class StockCSVTableIndex(enum.IntEnum):
    VOLUME              = 0
    OPEN                = 1
    HIGH                = 2
    LOW                 = 3
    CLOSE               = 4
    UP_DOWN             = 5
    CHANGES             = 6
    PE_RATIO            = 7
#}}}#
#{{{# csv getting
def get_twse_price_rpt(date_str):
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + date_str.replace('-','') + '&type=ALL')
    ret = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if len(i.split('",')) == 17 and i[0] != '='])), header=0)
    ret = ret.set_index('證券代號')
    ret = ret.drop(ret.columns[[0, 2, 3, 10, 11, 12, 13, 15]], axis=1)
    ret['成交股數'] = ret['成交股數'].str.replace(',', '')
    ret['開盤價']   = ret['開盤價'].str.replace(',', '')
    ret['最高價']   = ret['最高價'].str.replace(',', '')
    ret['最低價']   = ret['最低價'].str.replace(',', '')
    ret['收盤價']   = ret['收盤價'].str.replace(',', '')
    ret['本益比']   = ret['本益比'].str.replace(',', '')
    return ret

def get_twse_juristic_person_volume_rpt(date):
    date_str = str(date)
    r = requests.post('http://www.twse.com.tw/fund/T86?response=csv&date=' + date_str.replace('-','') + '&selectType=ALLBUT0999')
    ret = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n')[1:-1] if i[0] != '='])), header=0)
    ret = ret.set_index('證券代號')
    ret = ret.drop(ret.columns[[0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)

    date_axis = datetime.datetime(2017, 12, 17).date()
    if date >= date_axis:
        ret['外陸資買進股數(不含外資自營商)'] = ret['外陸資買進股數(不含外資自營商)'].str.replace(',', '')
        ret['外陸資賣出股數(不含外資自營商)'] = ret['外陸資賣出股數(不含外資自營商)'].str.replace(',', '')
    else:
        ret['外資買進股數'] = ret['外資買進股數'].str.replace(',', '')
        ret['外資賣出股數'] = ret['外資賣出股數'].str.replace(',', '')
        
    return ret

def get_twse_juristic_person_daily_volume_rpt(date):
    date_str = str(date)
    r = requests.post('http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=' + date_str.replace('-','') + '&type=day')
    ret = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n')[1:-1] if i[0] != '='])), header=0)
    ret = ret.set_index('單位名稱')

    ret['買進金額'] = ret['買進金額'].str.replace(',', '')
    ret['賣出金額'] = ret['賣出金額'].str.replace(',', '')
    ret['買賣差額'] = ret['買賣差額'].str.replace(',', '')
        
    return ret
#}}}#

## main
n_days = 1
date = datetime.datetime.now().date()
#date = datetime.datetime(2017, 4, 19).date()
#date = datetime.datetime(2017, 12, 25).date()
update_stock    = True
update_juristic = True

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
dbcur = stockdb.cursor()

if mysql_cmd.check_mysql_table_exists( stockdb, 'STOCKDB' ) == False:
    mysql_cmd.create_stockdb_table(dbcur)

if mysql_cmd.check_mysql_table_exists( stockdb, 'JURISTICDB' ) == False:
    mysql_cmd.create_juristicdb_table(dbcur)

fail_count = 1
date_count = 0
allow_continuous_fail_count = 10
while date_count < n_days:
    try:
        print('parsing', date)
        date_str = str(date)
        mysql_date = mysql_cmd.get_mysql_str2date_cmd(date_str)
        if update_juristic:
            print('parsing juristic data...')
            jur_vol_data = get_twse_juristic_person_daily_volume_rpt(date)
            if mysql_cmd.check_db_specific_date_exists(dbcur, 'JURISTICDB', date_str):
                print('warning: juristic data exists!')
                continue
            value = mysql_date + ", " + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['買進金額']) + ', ' + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['賣出金額']) + ', ' + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['買賣差額']) 

            mysql_cmd.insert_juristic_data_into_db(dbcur, value)
        ### end if
        if update_stock:
            price_data     = get_twse_price_rpt(date_str)
            juristic_data  = get_twse_juristic_person_volume_rpt(date)

            print('parsing stock data...')
            if mysql_cmd.check_db_specific_date_exists(dbcur, 'STOCKDB', date_str):
                print('warning: stock data exists!')
                fail_count += 1
                continue

            for stockid in price_data.index:
                if str(price_data.loc[stockid][StockCSVTableIndex.CLOSE]) != '--':
                    juristic_volume = 0
                    if stockid in juristic_data.index:
                        juristic_volume = int(juristic_data.loc[stockid][StockJuristicTableIndex.BUY_VOLUME]) - int(juristic_data.loc[stockid][StockJuristicTableIndex.SOLD_VOLUME]);
                        juristic_volume /= 1000.0
                    changes = float(price_data.loc[stockid][StockCSVTableIndex.CHANGES])
                    up_down = str(price_data.loc[stockid][StockCSVTableIndex.UP_DOWN]) 
                    if up_down == '-':
                        changes *= -1
                    elif up_down == 'nan':
                        up_down = 'x'

                    volume = float(price_data.loc[stockid][StockCSVTableIndex.VOLUME]) / 1000.0
                        
                    value = '\'%s\', %s, %s, %s, %s, %s, %s, %s, \'%s\', %s, %s' % \
                        (stockid, \
                         mysql_date, \
                         str(volume), \
                         str(price_data.loc[stockid][StockCSVTableIndex.OPEN]), \
                         str(price_data.loc[stockid][StockCSVTableIndex.HIGH]), \
                         str(price_data.loc[stockid][StockCSVTableIndex.LOW]), \
                         str(price_data.loc[stockid][StockCSVTableIndex.CLOSE]), \
                         str(changes), \
                         up_down, \
                         str(juristic_volume), \
                         str(price_data.loc[stockid][StockCSVTableIndex.PE_RATIO]), \
                        )
                    mysql_cmd.insert_stock_data_into_db(dbcur, value)
            ### end for
        ### end if
        stockdb.commit()
        fail_count = 0
    except:
        print('fail! check the date is holiday')
        fail_count += 1
        if fail_count == allow_continuous_fail_count:
            raise
            break
    
    # 減一天
    date_count += 1
    date -= datetime.timedelta(days=1)
    time.sleep(10)


stockdb.close()
