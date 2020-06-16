import pymysql
from DBUtils.PooledDB import PooledDB

from config import config


class DBConnPools(object):

    __poll = None

    def __init__(self):
        pass

    def __enter__(self):
        self.coon = self.__getConn()
        self.cursor = self.coon.cursor()
        print('创建coon和cursor')
        return self

    def __getConn(self, host, port, user, pwd, db, charset):
        if self.__poll is None:
            self.__poll = PooledDB(
                creator=pymysql, mincached=config.DB_MIN_CACHED, maxcached=config.DB_MAX_CACHED,
                maxshared=config.DB_MAX_SHARED, maxconnections=config.DB_MAX_CONNECYIONS,
                blocking=config.DB_BLOCKING, maxusage=config.DB_MAX_USAGE,
                setsession=config.DB_SET_SESSION,
                host=host, port=port, user=user, passwd=pwd, db=db, use_unicode=True,
                charset=charset
            )
        return self.__poll.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.coon.close()

        print('释放conn和cursor')

    def get_conn(self, host, port, user, pwd, db, charset):
        """
        从连接池当中，取出一个连接
        """
        conn = self.__getConn(host, port, user, pwd, db, charset)
        return conn


def getConn():
    x = DBConnPools()
    print(x)
    return DBConnPools()

