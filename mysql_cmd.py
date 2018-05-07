import pymysql

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

def create_stockdb_table(dbcur):
    dbcur.execute("""
                        CREATE TABLE STOCKDB (
                            stockid CHAR(16),
                            date DATE,
                            volume FLOAT,
                            open FLOAT,
                            high FLOAT,
                            low FLOAT,
                            close FLOAT,
                            changes FLOAT,
                            up_down CHAR(1),
                            juristic_volume FLOAT,
                            pe_ratio FLOAT
                        );
                  """)

def create_juristicdb_table(dbcur):
    dbcur.execute("""
                        CREATE TABLE JURISTICDB (
                            date DATE,
                            buy_volume  BIGINT,
                            sold_volume BIGINT,
                            diff_volume BIGINT
                        );
                  """)

def insert_stock_data_into_db(dbcur, stockid, date, volume, open_price, high_price, low_price, close_price, changes, up_down, juristic_volume, pe_ratio):
    value = '\'%s\', %s, %s, %s, %s, %s, %s, %s, \'%s\', %s, %s' % \
        (str(stockid), \
         str(date), \
         str(volume), \
         str(open_price), \
         str(high_price), \
         str(low_price), \
         str(close_price), \
         str(changes), \
         str(up_down), \
         str(juristic_volume), \
         str(pe_ratio) \
        )
    sdb_index = 'stockid, date, volume, open, high, low, close, changes, up_down, juristic_volume, pe_ratio'
    sql_cmd = 'INSERT INTO STOCKDB(' + sdb_index + ') VALUES(' + value + ');'
    dbcur.execute(sql_cmd)

def insert_juristic_data_into_db(dbcur, date, buy, sold, diff):
    value = '%s, %s, %s, %s' % (str(date), str(buy), str(sold), str(diff))
    jdb_index = 'date, buy_volume, sold_volume, diff_volume'
    sql_cmd = 'INSERT INTO JURISTICDB(' + jdb_index + ') VALUES(' + value + ');'
    dbcur.execute(sql_cmd)

def get_mysql_fetch_stock_data_cmd_between_period_date(stockid, time_period):
    sql_cmd = 'SELECT * FROM STOCKDB WHERE stockid = \'' + str(stockid) + \
              '\' AND DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ' + str(time_period) + ' DAY) AND CURRENT_DATE() ORDER BY date DESC;' 
    return sql_cmd

def get_mysql_str2date_cmd(date):
    return 'str_to_date(\'' + str(date) + '\',\'%Y-%m-%d\')'

def check_db_specific_date_exists(dbcur, table, date):
    sql_cmd = 'SELECT EXISTS(SELECT 1 FROM ' + table + ' WHERE date = ' + get_mysql_str2date_cmd(date) + ');'
    dbcur.execute(sql_cmd)
    row = dbcur.fetchall()
    return (row[0][0] == 1)

