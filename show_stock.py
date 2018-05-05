#!/usr/local/bin/python3

import os
import sys
sys.path.append(os.getcwd())

import talib
import pymysql
import itertools

import numpy as np
import pandas.io.sql as pdsql
import matplotlib.pyplot as plot

from twstock import codes



def get_mysql_fetch_cmd( stockid, time_period ):
    sql_cmd = 'SELECT * FROM STOCKDB WHERE stockid = \'' + str(stockid) + \
              '\' AND DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ' + str(time_period) + ' DAY) AND CURRENT_DATE() ORDER BY date;' 
    return sql_cmd

def get_period_days_data(df, period, maxmin, label):
    interval = np.array(df[label][::-1])
    if len(interval) > period:
        interval = interval[0:period]
    return maxmin(np.array(interval))

def show_juristic_plot( data_frame ):
    data_size = len(data_frame['juristic_volume'])
    summary_list = [sum(data_frame['juristic_volume'][0:i]) for i in range(0, data_size)]

    plot.bar(range(data_size), data_frame['juristic_volume'], label='juristic_volume', tick_label=data_frame['date'])
    #plot.bar(range(data_size), data_frame['juristic_volume'], label='juristic_volume')
    plot.plot(summary_list, label='summary', color='orange')
    plot.legend()

    plot.xticks(rotation=70)
    plot.show()

if len(sys.argv) < 2:
    print('please input stock id')
    sys.exit()

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
sid = sys.argv[1]

if codes[sid]:
    sql_cmd = get_mysql_fetch_cmd( stockid=sid, time_period=360 );
    data_frame = pdsql.read_sql_query(sql_cmd, stockdb)
    data_size  = len(data_frame)
    if len(sys.argv) == 3:
        period = int(sys.argv[2])
    else:
        period = data_size
    if data_size:
        print('公司名稱              : %9s' % codes[sid].name)
        print('最近交易日收盤價      : %11.2f' % np.array(data_frame['close'])[-1])
        print('最近交易日最低價      : %11.2f' % np.array(data_frame['low'])[-1])
        print('最近交易日最高價      : %11.2f' % np.array(data_frame['high'])[-1])
        print('最近交易日交易量      : %11.2f' % np.array(data_frame['volume'])[-1])
        print('最近交易日法人交易量  : %11.2f' % np.array(data_frame['juristic_volume'])[-1])
        print('最近交易日RSI         : %11.2f' % float(talib.RSI( np.array(data_frame['close']), timeperiod=5 )[-1]))
        print('')
        print('近5日收盤價           :', np.array(data_frame['close'])[::-1][0:6])
        print('法人近5日交易量       :', np.array(data_frame['juristic_volume'])[::-1][0:6])
        print('%-4d天法人交易總量    : %11.2f' % (data_size, sum(np.array(data_frame['juristic_volume']))))
        print('')
        for dd in [60, 120, 240]:
            if period >= dd:
                print('%3d 天均價            : %11.2f' % (dd, float(talib.MA( np.array(data_frame['close']), timeperiod=dd )[-1])))
                print('%3d 天最低價          : %11.2f' % (dd, get_period_days_data(data_frame, dd, min, 'low' )))
                print('%3d 天最高價          : %11.2f' % (dd, get_period_days_data(data_frame, dd, max, 'high')))
                print('%3d 天法人最大賣超量  : %11.2f' % (dd, get_period_days_data(data_frame, dd, min, 'juristic_volume')))
                print('%3d 天法人最大買超量  : %11.2f' % (dd, get_period_days_data(data_frame, dd, max, 'juristic_volume')))
                print('')
        ## show_juristic_plot(data_frame)
        ## df = np.array(data_frame[::-1])
        ## print(df.loc['close'])
        
    stockdb.close()
