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

def get_twse_juristic_person_daily_volumn_rpt(date):
    date_str = str(date)
    r = requests.post('http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=' + date_str.replace('-','') + '&type=day')
    ret = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n')[1:-1] if i[0] != '='])), header=0)
    ret = ret.set_index('單位名稱')

    ret['買進金額'] = ret['買進金額'].str.replace(',', '')
    ret['賣出金額'] = ret['賣出金額'].str.replace(',', '')
    ret['買賣差額'] = ret['買賣差額'].str.replace(',', '')
        
    return ret

## main
n_days = 5
#date = datetime.datetime.now().date()
date = datetime.datetime(2018, 4, 30).date()

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
dbcur = stockdb.cursor()

if check_mysql_table_exists( stockdb, 'JURISTICDB' ) == False:
    dbcur.execute("""
                        CREATE TABLE JURISTICDB (
                            date DATE,
                            buy_volumn  BIGINT,
                            sold_volumn BIGINT,
                            diff_volumn BIGINT
                        );
                  """)

fail_count = 1
date_count = 0
allow_continuous_fail_count = 10
index = 'date, buy_volumn, sold_volumn, diff_volumn'
while date_count < n_days:
    print('parsing', date)
    try:
        date_str        = str(date)
        juristic_data   = get_twse_juristic_person_daily_volumn_rpt(date)

        fail_count = 0

        mysql_date  = 'str_to_date(\'' + date_str + '\',\'%Y-%m-%d\')'
        sql_cmd     = "select exists(select 1 from JURISTICDB where date = " + mysql_date + ");"

        dbcur.execute(sql_cmd)
        row = dbcur.fetchall()
        if row[0][0] == 1:
            print('data exists!')
            break

        value = mysql_date + ", " + \
            str(juristic_data.loc['外資及陸資(不含外資自營商)']['買進金額']) + ', ' + \
            str(juristic_data.loc['外資及陸資(不含外資自營商)']['賣出金額']) + ', ' + \
            str(juristic_data.loc['外資及陸資(不含外資自營商)']['買賣差額']) 

        sql_cmd = 'INSERT INTO JURISTICDB(' + index + ') VALUES(' + value + ');'
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
