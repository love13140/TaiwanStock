import talib
import numpy as np

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

def KD5(data_frame):
    return talib.STOCH( high=normalize_list2talib(data_frame['high']), 
                            low=normalize_list2talib(data_frame['low']), 
                            close=normalize_list2talib(data_frame['close']),
                            fastk_period=5,
                            slowk_period=3,
                            slowk_matype=0,
                            slowd_period=3,
                            slowd_matype=0)

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
        signal += 'Close>MA20>MA60>MA120'
    elif close_overthan_MA20_overthan_MA60(data_frame['close']):
        signal += 'Close>MA20>MA60'
    elif close_overthan_MA60_overthan_MA120(data_frame['close']):
        signal += 'Close>MA60>MA120'
    else:
        return False

    RSI_signal = True
    rsi5 = RSI5(data_frame['close'])
    if RSI5_between_28_and_18(data_frame['close']):
        signal += ', RSI 顯示超賣:%4.2f' % rsi5
    elif RSI5(data_frame['close']) > 85:
        signal += ', RSI 顯示超買:%4.2f' % rsi5
        RSI_signal = False
    else:
        RSI_signal = False


    KD_signal = True;
    slowk, slowd = KD5(data_frame)
    if slowk[-1] > slowd[-1] and slowk[-2] < slowd[-2]:
        signal += ', KD 黃金交叉 (%4.2f, %4.2f)' % (slowk[-1], slowd[-1])
    if slowk[-1] < 15 or slowd[-1] < 15:
        signal += ', KD 顯示超賣 K:%4.2f' % slowk[-1]
    elif slowk[-1] > 85 or slowd[-1] > 85:
        signal += ', KD 顯示超買 K:%4.2f' % slowk[-1]
        KD_signal = False;
    else:
        KD_signal = False;
    

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

    if RSI_signal or KD_signal or juristic_signal:
        return signal
    else:
        return False

