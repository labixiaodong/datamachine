from utils.db_pools import getDBonnectionPools
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
            self.dbinstance = getDBonnectionPools()
