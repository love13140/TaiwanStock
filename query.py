#!/usr/local/bin/python3
import os
import sys
sys.path.append(os.getcwd())

import analysis
import mysql_cmd

import talib
import enum
import pymysql
import numpy as np
import pandas.io.sql as pdsql
from twstock import codes

## main 

## [::-1]  reverse, for TALib
stock_list = {s for s, v in codes.items() if v.type == '股票' and v.market == '上市'}

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
stock_list = sorted(stock_list)
for sid in stock_list:
    sql_cmd = mysql_cmd.get_mysql_fetch_stock_data_cmd_between_period_date(stockid=sid, time_period=180);
    data_frame = pdsql.read_sql_query(sql_cmd, stockdb)
    if len(data_frame):

        ###  0, last date
        ### -1, earlest date

        signal = analysis.get_buy_signal(data_frame)
        if signal:
            rsi5  = analysis.RSI5(data_frame['close'])
            ma20  = analysis.MA20(data_frame['close'])
            ma60  = analysis.MA60(data_frame['close'])
            ma120 = analysis.MA120(data_frame['close'])
            
            jtl   = [int(i) for i in data_frame['juristic_volumn'][0:3]]
            cl    = data_frame['close'][0:3]

            signal1day = analysis.get_buy_signal(data_frame.drop(data_frame.index[0]).reset_index(drop=True))
            signal2day = analysis.get_buy_signal(data_frame.drop(data_frame.index[[0,1]]).reset_index(drop=True))
            signal3day = analysis.get_buy_signal(data_frame.drop(data_frame.index[[0,1,2]]).reset_index(drop=True))
            signal4day = analysis.get_buy_signal(data_frame.drop(data_frame.index[[0,1,2,3]]).reset_index(drop=True))
    
            if signal1day and signal2day and signal3day and signal4day:
                signal += ', 連續五日買進信號'
            elif signal1day and signal2day and signal3day:
                signal += ', 連續四日買進信號'
            elif signal1day and signal2day:
                signal += ', 連續三日買進信號'
            elif signal1day:
                signal += ', 連續二日買進信號'

            print('%s: Close3D=[%7.2f, %7.2f, %7.2f], MA20/60=[%7.2f, %7.2f], Juristic3D=[%5d, %5d, %5d] : %s'
                     % (sid, cl[0], cl[1], cl[2], ma20, ma60, jtl[0], jtl[1], jtl[2], signal))
stockdb.close()
