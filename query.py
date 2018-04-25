#!/usr/local/bin/python3

import talib
import numpy as np
import pymysql
import enum
import pandas.io.sql as pdsql
from twstock import codes

class MySQLTableIndex(enum.IntEnum):
    STOCKID             = 0
    DATE                = 1
    VOLUMN              = 2
    OPEN                = 3
    HIGH                = 4
    LOW                 = 5
    CLOSE               = 6
    CHANGES             = 7
    UP_DOWN             = 8
    JURISTIC_VOLUMN     = 9
    PE_RATIO            = 10
##
def normalize_list2talib( data_list ):
    return np.array(data_list[::-1])

## MA
def MA5( close_list ):
    return float(talib.MA( normalize_list2talib(close_list), timeperiod=5 )[-1])
def MA20( close_list ):
    return float(talib.MA( normalize_list2talib(close_list), timeperiod=20 )[-1])
def MA60( close_list ):
    return float(talib.MA( normalize_list2talib(close_list), timeperiod=60 )[-1])
def MA120( close_list ):
    return float(talib.MA( normalize_list2talib(close_list), timeperiod=120 )[-1])
def MA240( close_list ):
    return float(talib.MA( normalize_list2talib(close_list), timeperiod=240 )[-1])

## RSI
def RSI5( close_list ):
    return float(talib.RSI(  normalize_list2talib(close_list), timeperiod=5 )[-1])
def RSI10( close_list ):
    return float(talib.RSI(  normalize_list2talib(close_list), timeperiod=10 )[-1])
def RSI20( close_list ):
    return float(talib.RSI(  normalize_list2talib(close_list), timeperiod=20 )[-1])

def RSI_between_interval( close_list, time_period, max_rsi, min_rsi ):
    rsi = float(talib.RSI( normalize_list2talib(close_list), timeperiod=time_period )[-1])
    return max_rsi >= rsi >= min_rsi
        
def RSI5_between_28_and_18( close_list ):
        return RSI_between_interval( close_list, time_period=5, max_rsi=28, min_rsi=18)

def juristic_volumn_buysuper_5_days_strong( juristic_volumn ):
    return juristic_volumn[0] >= juristic_volumn[1] >= juristic_volumn[2] >= juristic_volumn[3] >= juristic_volumn[4] >= 50

def juristic_volumn_buysuper_5_days( juristic_volumn ):
    return juristic_volumn[0] > 100 and  juristic_volumn[1] > 100 and  juristic_volumn[2] > 100 and  juristic_volumn[3] > 100 and  juristic_volumn[4] > 100

def juristic_volumn_buysuper_4_days_strong( juristic_volumn ):
    return juristic_volumn[0] >= juristic_volumn[1] >= juristic_volumn[2] >= juristic_volumn[3] >=  50

def juristic_volumn_buysuper_4_days( juristic_volumn ):
    return juristic_volumn[0] > 100 and  juristic_volumn[1] > 100 and  juristic_volumn[2] > 100 and  juristic_volumn[3] > 100

def juristic_volumn_buysuper_3_days_strong( juristic_volumn ):
    return juristic_volumn[0] >= juristic_volumn[1] >= juristic_volumn[2] >= 50

def juristic_volumn_buysuper_3_days( juristic_volumn ):
    return juristic_volumn[0] > 100 and  juristic_volumn[1] > 100 and  juristic_volumn[2] > 100

    
def juristic_volumn_buysuper_reverse( juristic_volumn ):
    return juristic_volumn[0] >= 50 >= juristic_volumn[1] > 0 > juristic_volumn[2]

def close_rise_5_days(close_list):
    return close_list[0] > close_list[1] > close_list[2] > close_list[3] > close_list[4]

def close_rise_3_days(close_list):
    return close_list[0] > close_list[1] > close_list[2]

def close_overthan_MA60_overthan_MA120( close_list ):
    ma60  = MA60( close_list )
    ma120 = MA120( close_list )
    return close_list[0] > ma60 > ma120

def close_overthan_MA20_overthan_MA60( close_list ):
    ma20 = MA20( close_list )
    ma60 = MA60( close_list )
    return close_list[0] > ma20 > ma60

def get_buy_signal(data_frame):
    if data_frame['volumn'][0] < 1000:
        return False
    if data_frame['juristic_volumn'][0] == data_frame['juristic_volumn'][1] == data_frame['juristic_volumn'][2] == 0:
        return False

    signal = ''
    if close_overthan_MA20_overthan_MA60(data_frame['close']) and close_overthan_MA60_overthan_MA120(data_frame['close']):
        signal += '收盤價>MA20>MA60>MA120'
    elif close_overthan_MA20_overthan_MA60(data_frame['close']):
        signal += '收盤價>MA20>MA60'
    elif close_overthan_MA60_overthan_MA120(data_frame['close']):
        signal += '收盤價>MA60>MA120'
    else:
        return False

    if RSI5_between_28_and_18(data_frame['close']):
        signal += ', RSI 顯示超賣'
        RSI_signal = True
    else:
        RSI_signal = False

    if RSI5(data_frame['close']) > 85:
        signal += ', RSI 顯示超買需注意'

    juristic_signal = True
    if juristic_volumn_buysuper_5_days_strong(data_frame['juristic_volumn']):
        signal += ', 法人連續5日強力買超'
    elif juristic_volumn_buysuper_5_days(data_frame['juristic_volumn']):
        signal += ', 法人連續5日買超'
    elif juristic_volumn_buysuper_4_days_strong(data_frame['juristic_volumn']):
        signal += ', 法人連續4日強力買超'
    elif juristic_volumn_buysuper_4_days(data_frame['juristic_volumn']):
        signal += ', 法人連續4日買超'
    elif juristic_volumn_buysuper_3_days_strong(data_frame['juristic_volumn']):
        signal += ', 法人連續3日強力買超'
    elif juristic_volumn_buysuper_3_days(data_frame['juristic_volumn']):
        signal += ', 法人連續3日買超'
    elif juristic_volumn_buysuper_reverse(data_frame['juristic_volumn']):
        signal += ', 法人逆轉買超'
    else:
        juristic_signal = False

    if close_rise_5_days(data_frame['close']):
        signal += ', 連續5日上漲'
    elif close_rise_3_days(data_frame['close']):
        signal += ', 連續3日上漲'

    if RSI_signal or juristic_signal:
        return signal
    else:
        return False

def get_mysql_fetch_cmd( stockid, time_period ):
    sql_cmd = 'SELECT * FROM STOCKDB WHERE stockid = \'' + str(stockid) + \
              '\' AND DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ' + str(time_period) + ' DAY) AND CURRENT_DATE() ORDER BY date DESC;' 
    return sql_cmd

## main 

## [::-1]  reverse, for TALib
stock_list = {s for s, v in codes.items() if v.type == '股票' and v.market == '上市'}

stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
stock_list = sorted(stock_list)
for sid in stock_list:
    sql_cmd = get_mysql_fetch_cmd( stockid=sid, time_period=180 );
    data_frame = pdsql.read_sql_query(sql_cmd, stockdb)
    if len(data_frame):

        ###  0, last date
        ### -1, earlest date

        signal = get_buy_signal(data_frame)
        if signal:
            rsi5  = RSI5(data_frame['close'])
            ma20  = MA20(data_frame['close'])
            ma60  = MA60(data_frame['close'])
            ma120 = MA120(data_frame['close'])
            
            jtl   = [int(i) for i in data_frame['juristic_volumn'][0:3]]
            cl    = data_frame['close'][0:3]

            signal1day = get_buy_signal(data_frame.drop(data_frame.index[0]).reset_index(drop=True))
            signal2day = get_buy_signal(data_frame.drop(data_frame.index[[0,1]]).reset_index(drop=True))
            signal3day = get_buy_signal(data_frame.drop(data_frame.index[[0,1,2]]).reset_index(drop=True))
            signal4day = get_buy_signal(data_frame.drop(data_frame.index[[0,1,2,3]]).reset_index(drop=True))
    
            if signal1day and signal2day and signal3day and signal4day:
                signal += ', 連續五日買進信號'
            elif signal1day and signal2day and signal3day:
                signal += ', 連續四日買進信號'
            elif signal1day and signal2day:
                signal += ', 連續三日買進信號'
            elif signal1day:
                signal += ', 連續二日買進信號'

            print('%s: RSI5=%4.2f, Close=%6.2f, %6.2f, %6.2f, MA20=%6.2f, MA60=%6.2f, 法人三日買賣=%5d, %5d, %5d : %s'
                     % (sid, rsi5, cl[0], cl[1], cl[2], ma20, ma60, jtl[0], jtl[1], jtl[2], signal))
stockdb.close()
