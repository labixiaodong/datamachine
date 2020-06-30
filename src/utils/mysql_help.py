from utils.db_pools import DBConnPools
import pandas as pd
from config import config
import pymysql
import os
import time


DB = config.big_data
host = DB["host"]
port = DB["port"]
user = DB["user"]
passwd = DB["passwd"]
db = DB["db"]


class MysqlHelper(object):

    def __init__(self,
                 host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8', read_timeout=5400, pools=False):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        self.read_timeout = read_timeout
        self.pools = pools
        self.conn = None
        self.cursor = None

        if self.pools:
            self.dbinstance = DBConnPools()

    def insertdata_bydf(self, df, tb, if_exists='append', n=5):

        start_time = time.time()   # 返回当前时间戳

        # 对传入的df进行清洗，替换nan为mysql接受的None，并且转为字符串
        df = df.where(pd.notnull(df), "None").replace("nan", "None").replace("NaN", "None")
        df = df.astype("str")

        # 组合sql语句
        sql = """insert into {0} ({1}) values ({2});"""
        sql = sql.format(tb, ",".join(df.columns), ("%s," * len(df.columns))[:-1])

        count_times = 0
        while count_times < n:
            try:
                conn = self.getconn()
                cursor = conn.cursor()

                if if_exists == 'replace':
                    delete_rows = cursor.execute('delete from {0}'.format(tb))
                elif if_exists == 'replace-truncate':
                    delete_rows = cursor.execute('truncate from {0}'.format(tb))
                else:
                    delete_rows = 0

                para = [tuple([None if y == "None" else y for y in x]) for x in df.values]
                insert_rows = cursor.executemany(sql, para)
                conn.commit()

                print("   insert数据行数:{0}".format(insert_rows))
                print("数据库insert成功")

                endTime = time.time()
                time_eclipse = round((endTime - start_time), 2)

                print("插入数据耗时{0}".format(time_eclipse))

                count_times = n

            except Exception as e:

                count_times += 1
                conn.rollback()
                conn.commit()
                self.close(conn, cursor)

                time.sleep(10)

                if count_times >= n:
                    print('insertmany_bydf error execute:')
                    raise Exception("数据插入失败")

    def getconn(self):
        if self.pools:
            conn = self.dbinstance.get_conn(self.host, self.port, self.user, self.passwd, self.db, self.charset)
        else:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.db,
                read_timeout=self.read_timeout,
                charset=self.charset,
            )
            print("\n数据库连接成功")
        return conn

    def close(self, conn, cursor=None):
        try:
            if cursor is not None:
                cursor.close()
            conn.close()
            if not self.pools:
                print("数据库连接关闭")
        except:
            pass



