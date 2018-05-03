#!/usr/local/bin/python3
import os
import sys
sys.path.append(os.getcwd())

import mysql_cmd

import time
import pymysql
import datetime
import requests
import numpy as np
import pandas as pd
from io import StringIO

#{{{# exxcel getting
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

def get_twse_juristic_person_volumn_rpt(date):
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

def get_twse_juristic_person_daily_volumn_rpt(date):
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
#date = datetime.datetime.now().date()
date = datetime.datetime(2017, 4, 21).date()
update_juristic = False

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
sdbcur = stockdb.cursor()

if mysql_cmd.check_mysql_table_exists( stockdb, 'STOCKDB' ) == False:
    mysql_cmd.create_stockdb_table(sdbcur)

if mysql_cmd.check_mysql_table_exists( stockdb, 'JURISTICDB' ) == False:
    mysql_cmd.create_stockdb_table(sdbcur)

fail_count = 1
date_count = 0
allow_continuous_fail_count = 10
while date_count < n_days:
    print('parsing', date)
    try:
        date_str        = str(date)
        price_data      = get_twse_price_rpt(date_str)
        juristic_data   = get_twse_juristic_person_volumn_rpt(date)
        if update_juristic:
            jur_vol_data = get_twse_juristic_person_daily_volumn_rpt(date)

        fail_count = 0
        mysql_date = mysql_cmd.get_mysql_str2date_cmd(date_str)
        print('parsing stock data...')
        for stockid in price_data.index:
            sql_cmd = "SELECT EXISTS(SELECT 1 FROM STOCKDB WHERE stockid = \'" + stockid + "\' AND date = " + mysql_date + ");"
            sdbcur.execute(sql_cmd)
            row = sdbcur.fetchall()
            if row[0][0] == 1:
                print('warning: stock data exists!')
                fail_count += 1
                break
            if str(price_data.loc[stockid][4]) != '--':
                juristic_volumn = 0
                if stockid in juristic_data.index:
                    juristic_volumn = int(juristic_data.loc[stockid][0]) - int(juristic_data.loc[stockid][1]); #法人買賣超
                    juristic_volumn /= 1000.0
                changes = float(price_data.loc[stockid][6])
                up_down = str(price_data.loc[stockid][5]) 
                if up_down == '-':
                    changes *= -1
                elif up_down == 'nan':
                    up_down = 'x'

                volumn = float(price_data.loc[stockid][0]) / 1000.0
                    
                value = '\'' + stockid                          + '\','  + mysql_date                       + ', '   + str(volumn ) + \
                        ', ' + str(price_data.loc[stockid][1] ) + ', '   + str(price_data.loc[stockid][2] ) + ', '   + str(price_data.loc[stockid][3] ) + \
                        ', ' + str(price_data.loc[stockid][4] ) + ', '   + str(changes) + ', \'' + up_down + '\','   + str(juristic_volumn)           + \
                        ', ' + str(price_data.loc[stockid][7] )
                
                mysql_cmd.insert_stock_data_into_db(sdbcur, value)
        ### end for
        if update_juristic:
            print('parsing juristic data...')
            sql_cmd = 'SELECT EXISTS(SELECT 1 FROM JURISTICDB WHERE date = ' + mysql_date + ');'
            sdbcur.execute(sql_cmd)
            row = sdbcur.fetchall()
            if row[0][0] == 1:
                print('warning: juristic data exists!')
                break

            value = mysql_date + ", " + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['買進金額']) + ', ' + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['賣出金額']) + ', ' + \
                str(jur_vol_data.loc['外資及陸資(不含外資自營商)']['買賣差額']) 

            mysql_cmd.insert_juristic_data_into_db(dbcur, value)

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

stockdb.commit()
stockdb.close()
