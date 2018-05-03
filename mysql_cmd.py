import pymysql
import enum

class StockTableIndex(enum.IntEnum):
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

def create_juristicdb_table(dbcur):
    dbcur.execute("""
                        CREATE TABLE JURISTICDB (
                            date DATE,
                            buy_volumn  BIGINT,
                            sold_volumn BIGINT,
                            diff_volumn BIGINT
                        );
                  """)

def insert_stock_data_into_db(dbcur, value):
    sdb_index = 'stockid, date, volumn, open, high, low, close, changes, up_down, juristic_volumn, pe_ratio'
    sql_cmd = 'INSERT INTO STOCKDB(' + sdb_index + ') VALUES(' + value + ');'
    dbcur.execute(sql_cmd)

def insert_juristic_data_into_db(dbcur, value):
    jdb_index = 'date, buy_volumn, sold_volumn, diff_volumn'
    sql_cmd = 'INSERT INTO JURISTICDB(' + jdb_index + ') VALUES(' + value + ');'
    sdbcur.execute(sql_cmd)

def get_mysql_fetch_stock_data_cmd_between_period_date(stockid, time_period):
    sql_cmd = 'SELECT * FROM STOCKDB WHERE stockid = \'' + str(stockid) + \
              '\' AND DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ' + str(time_period) + ' DAY) AND CURRENT_DATE() ORDER BY date DESC;' 
    return sql_cmd

def get_mysql_str2date_cmd(date):
    return 'str_to_date(\'' + str(date) + '\',\'%Y-%m-%d\')'
