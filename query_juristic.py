#!/usr/local/bin/python3

import pymysql
import numpy as np
import pandas.io.sql as pdsql
import matplotlib.pyplot as plot


def get_mysql_fetch_cmd( time_period ):
    #sql_cmd = 'SELECT * FROM JURISTICDB WHERE DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ' \
    #            + str(time_period) + ' DAY) AND CURRENT_DATE() ORDER BY date;' 
    sql_cmd = 'SELECT * FROM JURISTICDB WHERE DATE(date) BETWEEN str_to_date(\'2018-01-01\',\'%Y-%m-%d\' ) AND CURRENT_DATE() ORDER BY date;' 
    return sql_cmd

## main 
stockdb = pymysql.connect(db = 'stockdb', user='root', passwd='root', host='localhost', unix_socket='/tmp/mysql.sock')
sql_cmd = get_mysql_fetch_cmd( time_period=360 )
data_frame = pdsql.read_sql_query(sql_cmd, stockdb)
stockdb.close()

if len(data_frame):
    data_size = len(data_frame['diff_volumn'])
    summary_list = [sum(data_frame['diff_volumn'][0:i]) for i in range(0, data_size)]
    print('summary from 2018/01/01 : {:,}'.format(sum(data_frame['diff_volumn'])))

    plot.bar(range(data_size), data_frame['diff_volumn'], label='diff_volumn', tick_label=data_frame['date'])
    #plot.bar(range(data_size), data_frame['diff_volumn'], label='diff_volumn')
    plot.plot(summary_list, label='summary', color='orange')
    plot.legend()

    plot.xticks(rotation=70)
    plot.show()


