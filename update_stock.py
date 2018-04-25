#!/usr/local/bin/python3

import datetime
import time
import requests
import pymysql
import pandas as pd
import numpy as np
from enum import Enum
from io import StringIO

def check_mysql_table_exists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
                          SELECT COUNT(*)
                          FROM information_schema.tables
                          WHERE table_name = '{0}'
                          """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

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

## main
n_days = 1 
date = datetime.datetime.now().date()
#date = datetime.datetime(2017, 10, 15).date()

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
dbcur = stockdb.cursor()

if check_mysql_table_exists( stockdb, 'STOCKDB' ) == False:
    dbcur.execute("""
                        CREATE TABLE STOCKDB (
                            stockid CHAR(16),
                            date DATE,
                            volumn FLOAT,
                            open FLOAT,
                            high FLOAT,
                            low FLOAT,
                            close FLOAT,
                            changes FLOAT,
                            up_down CHAR(1),
                            juristic_volumn FLOAT,
                            pe_ratio FLOAT
                        );
                  """)

fail_count = 1
date_count = 0
allow_continuous_fail_count = 10
index = 'stockid, date, volumn, open, high, low, close, changes, up_down, juristic_volumn, pe_ratio'
while date_count < n_days:
    print('parsing', date)
    try:
        # 抓資料
        date_str        = str(date)
        price_data      = get_twse_price_rpt(date_str)
        juristic_data   = get_twse_juristic_person_volumn_rpt(date)

        fail_count = 0
        for stockid in price_data.index:
            mysql_date  = 'str_to_date(\'' + date_str + '\',\'%Y-%m-%d\')'
            sql_cmd     = "select exists(select 1 from STOCKDB where stockid = \'" + stockid + "\' AND date = " + mysql_date + ");"

            dbcur.execute(sql_cmd)
            row = dbcur.fetchall()
            if row[0][0] == 1:
                print('data exists!')
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
                    
                value = '\'' + stockid                          + '\','  + mysql_date                       + ", "   + str(volumn ) + \
                        ", " + str(price_data.loc[stockid][1] ) + ", "   + str(price_data.loc[stockid][2] ) + ", "   + str(price_data.loc[stockid][3] ) + \
                        ", " + str(price_data.loc[stockid][4] ) + ", "   + str(changes) + ", \'" + up_down + "\',"   + str(juristic_volumn)           + \
                        ", " + str(price_data.loc[stockid][7] )

            sql_cmd = 'INSERT INTO STOCKDB(' + index + ') VALUES(' + value + ');'
            dbcur.execute(sql_cmd)
            stockdb.commit()

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
