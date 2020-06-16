# -*- coding: utf-8 -*-
# @Time    : 2018/7/17 18:38
# @Author  : Will_Ji
# @Email   : 864311352@qq.com
# @File    : mysqlhelper.py.py
# @Software: PyCharm


from utils.decorators import elapse
from utils.mydbpools import getDBonnectionPools
import pandas as pd
from config import config
import pymysql
from utils.inforobot import dingdingrobot
import multiprocessing
from math import ceil
from utils.encryption import encryption
import re
from utils.try_rerun import try_rerun
from utils.hflog import hflog
import os
import time
import sys
from queue import Queue
import threading
from utils.stringhelper import StringHelper

sys.path.append("../")

DB = config.tm_new_hfjy
host = DB["host"]
port = DB["port"]
user = DB["user"]
passwd = DB["passwd"]
db = DB["db"]

encryption_instance = encryption()


class mysqlhelper(object):
    def __init__(
            self, host=host, port=port, user=user, passwd=passwd, db=db, charset="utf8", read_timeout=5400, pools=False
    ):

        self.host = encryption_instance.decrypt(host)
        self.port = int(encryption_instance.decrypt(port))
        self.user = encryption_instance.decrypt(user)
        self.passwd = encryption_instance.decrypt(passwd)
        self.db = encryption_instance.decrypt(db)
        self.charset = charset
        self.read_timeout = read_timeout
        self.pools = pools
        self.conn = None
        self.cursor = None

        if self.pools:
            self.dbinstance = getDBonnectionPools()

    @elapse(dingding=False, des="   从数据库insert数据")
    def insertmany_bydf(self, df, tb, if_exists="append", n=6):
        """数据插入数据库的封装方法用于`处理空值&打印过程信息&打印插入信息`"""

        startTime = time.time()

        filename = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:].split(".py")[0].split("/")[-1]

        df = df.where(pd.notnull(df), "None").replace("nan", "None").replace("NaN", "None")
        df = df.astype("str")
        sql = """insert into {0} ({1}) values ({2});"""
        sql = sql.format(tb, ",".join(df.columns), ("%s," * len(df.columns))[:-1])
        count_times = 0

        while count_times < n:
            try:

                conn = self.getconn()
                cursor = conn.cursor()

                if if_exists == "replace":
                    delete_rows = cursor.execute("delete from {0}".format(tb))
                elif if_exists == "replace-truncate":
                    delete_rows = cursor.execute("truncate {0}".format(tb))
                else:
                    delete_rows = 0
                para = [tuple([None if y == "None" else y for y in x]) for x in df.values]
                insert_rows = cursor.executemany(sql, para)
                conn.commit()

                print("   insert数据行数:{0}".format(insert_rows))
                print("数据库insert成功")

                endTime = time.time()
                time_eclipse = round((endTime - startTime), 2)

                hflog.info(
                    {
                        "filename": filename,
                        "tb": tb,
                        "delete_rows": delete_rows,
                        "insert_rows": insert_rows,
                        "time_eclipse": time_eclipse,
                    }
                )

                count_times = n

            except Exception as e:

                count_times += 1
                conn.rollback()
                conn.commit()
                self.close(conn, cursor)

                time.sleep(10)

                if count_times >= n:
                    print('insertmany_bydf error execute:')
                    content = repr(e)
                    error_content = StringHelper.error(content)
                    print(error_content)

                    dingdingrobot(content=error_content, subject='alarm')

                    print("分割线end--------我分割我分割----------分割线end")

                    raise Exception("数据插入失败")



    @elapse(dingding=False, des="   byengin从数据库insert数据")
    def insertmany_bydf_thread(self, big_df, tb, if_exists="append", pools=True):

        self.pools = pools
        self.dbinstance = getDBonnectionPools()

        if if_exists == "replace":
            self.execute(sql="truncate {0}".format(tb))

        q = Queue(maxsize=15)  # 设定最大队列数和线程数

        nrows = big_df.shape[0]
        cut = 50000
        nceil = ceil(nrows / 50000)

        for i in range(nceil):
            t = threading.Thread(target=self.insertmany_bydf, args=(big_df[cut * i: cut * (i + 1)], tb, "append"))
            q.put(t)
            if (q.full() == True) or i == nceil - 1:
                thread_list = []
                while q.empty() == False:
                    t = q.get()
                    thread_list.append(t)
                    t.start()
                for t in thread_list:
                    t.join()

    @elapse(dingding=False, des="   从数据库get数据")
    @try_rerun(dingding=True, n=config.try_rerun_mysql_n, sleep_time=config.try_rerun_mysql_sleep)
    def __get_df(self, sql, index=0, toprint=None, connect_once=True):
        sql = self.sql_clean(sql)

        try:
            if self.conn.open == False:
                self.conn = self.getconn()
        except:
            self.conn = self.getconn()
        try:
            self.cursor = self.conn.cursor()
            set_sql = "SET SESSION group_concat_max_len = 102400;"
            rows = self.cursor.execute(set_sql)
            df = pd.read_sql(sql, self.conn)
            self.to_print(df, index=index, toprint=toprint)
            if not connect_once:
                self.close(self.conn, self.cursor)
        except Exception as e:
            print("连接异常")
            self.close(self.conn, self.cursor)
            ## 判断错误如果为sql语法错误的话，跳出循环

            ## 错误内容
            content = repr(e)
            error_content = StringHelper.error(content)

            if StringHelper.sql_error_check(content):
                dingdingrobot(
                    content="sql语法不正确\n{0}\n路径try_rerun".format(error_content),
                    subject="alarm的堂哥")
                raise

        return df

    def get_df(self, *args, toprint=None, connect_once=True):
        dfs = []

        for i, sql in enumerate(args):
            df = self.__get_df(sql, index=i, toprint=toprint, connect_once=connect_once)
            dfs.append(df)
        if connect_once:
            self.close(self.conn, self.cursor)

        return dfs if len(dfs) > 1 else dfs[0]

    def to_print(self, df, index=0, toprint=None):
        print("   get[{0}]原始数据shape:{1}".format(index + 1, df.shape))
        if toprint:
            if isinstance(toprint, str):
                toprint = [toprint]
            for method in toprint:
                mtd = getattr(df, method)
                if callable(mtd):
                    print("   get原始数据{0}:\n{1}".format(method, mtd()))
                else:
                    print("   get原始数据{0}:\n{1}".format(method, mtd))

    def get_df_loadfile(self, *file_names, toprint=None, encoding="utf-8", format=None, connect_once=True):
        sqls = []
        for file_name in file_names:
            print("***开始执行sql文件:{0}***".format(file_name))
            with open("{0}".format(file_name), "r", encoding=encoding) as f:
                lines = f.read()
                if format:
                    if not isinstance(format, list):
                        format = [format]
                    lines = lines.format(*format)
                sqllist = lines.split(";\n")
                for sql in sqllist:
                    if sql.strip():
                        sql = self.sql_clean(sql)
                        sqls.append(sql.strip())
        dfs = self.get_df(*sqls, toprint=toprint, connect_once=connect_once)
        if not isinstance(dfs, list):
            dfs = [dfs]
        return dfs if len(dfs) > 1 else dfs[0]

    @try_rerun(dingding=True)
    def updatemany_bydf(self, df, sql, batch=False, multip=False, processes=4):
        df = df.where(pd.notnull(df), "None")
        df = df.astype("str")

        if not multip:

            conn = self.getconn()
            cursor = conn.cursor()
            try:
                sql = sql
                para = [[None if y == "None" else y for y in x] for x in df.values]

                if not batch:
                    rows = cursor.executemany(sql, para)
                    print("   update数据行数:{0}".format(rows))
                    print("数据库update完成")
                else:
                    nrows = df.shape[0]
                    nceil = ceil(nrows / 100) + 1

                    for i in range(nceil):
                        if 100 * (i + 1) < nrows:
                            rows = cursor.executemany(sql, para[100 * i: 100 * (i + 1)])
                        else:
                            rows = cursor.executemany(sql, para[100 * i: 100 * (i + 1)])
                        print("   update数据行数[{0}]:{1}".format(i, rows))
                        print("数据库update完成")

            except Exception as e:
                print("分割线start--------我分割我分割----------分割线start")
                print(para)

                print('updatemany_bydf error execute:')
                content = repr(e)
                error_content = StringHelper.error(content)
                print(error_content)

                conn.rollback()
                print("分割线end--------我分割我分割----------分割线end")
            conn.commit()
            self.close(conn, cursor)
        else:
            self.pools = True
            if self.pools:
                self.dbinstance = getDBonnectionPools()
            processes = processes
            interval = 100
            cut = df.shape[0] // interval + 1

            p = multiprocessing.Pool(processes=processes)

            for i in range(cut):
                tep = df[interval * i: interval * (i + 1)].reset_index(drop=True)
                if tep.shape[0] > 0:
                    print(i)
                    p.apply_async(self.updatemany_bydf(tep, sql, multip=False))
                else:
                    break
            p.close()
            p.join()

    @elapse(dingding=False, des="   byengin从数据库insert数据")
    @try_rerun(dingding=True)
    def insertmany_byengin(self, df, tb, if_exists="append"):

        from sqlalchemy import create_engine

        """使用pymysql模块,初始化数据库连接"""
        db_info = {
            "user": self.user,
            "passwd": self.passwd,
            "host": self.host,
            "port": self.port,
            "database": self.db,
            "charset": self.charset,
        }

        engine = create_engine(
            "mysql+pymysql://%(user)s:%(passwd)s@%(host)s:%(port)d/%(database)s?charset=%(charset)s" % db_info
        )

        """将新建的DataFrame储存为MySQL中的数据表，不储存index列(index=False)
        if_exists:
        1.fail:如果表存在，啥也不做
        2.replace:如果表存在，删了表记录，重新把数据插入
        3.append:如果表存在，把数据插入，如果表不存在创建一个表！！
        4.new：新建表，并插入数据
        """

        if if_exists == "replace":
            self.execute(sql="truncate {0}".format(tb))
            if_exists = "append"

        # import io
        #
        # output = io.StringIO()
        # # ignore the index
        # df.to_csv(output, index=False, header=False)
        # output.getvalue()
        # # # jump to start of stream
        # # output.seek(0)
        #
        # connection = engine.raw_connection()  # engine 是 from sqlalchemy import create_engine
        # cursor = connection.cursor()
        # # null value become ''
        # rows = cursor.copy_from(output, tb)
        # print(rows)
        # connection.commit()
        # cursor.close()

        pd.io.sql.to_sql(df, tb, con=engine, index=False, if_exists=if_exists, chunksize=102400, method="multi")
        print("Write to MySQL successfully!")

    def execute(self, sql, params=None, index=0):

        startTime = time.time()
        filename = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:].split(".py")[0].split("/")[-1]

        conn = self.getconn()
        cursor = conn.cursor()
        sql = self.sql_clean(sql)
        try:
            rows = cursor.execute(sql)
            conn.commit()
            if re.search("(delete)|(truncate)|(drop)", sql, flags=re.I):
                print("   execute[{0}]删除表行数:{1}".format(index, rows))
                endTime = time.time()
                time_eclipse = round((endTime - startTime), 2)
                hflog.info({"filename": filename, "sql": sql, "rows": rows, "time_eclipse": time_eclipse})
            elif re.search("create", sql, flags=re.I):
                print("   execute[{0}]创建表成功".format(index))
            elif re.search("update", sql, flags=re.I):
                print("   execute[{0}]更新表行数:{1}".format(index, rows))
            else:
                print("   execute[{0}]数据行数:{1}".format(index, rows))
            endTime = time.time()
            time_eclipse = round((endTime - startTime), 2)
            print("sql语句执行成功" + ", 执行耗时: " + str(time_eclipse) + " s")
            self.close(conn, cursor)
        except Exception as e:
            print("分割线start--------我分割我分割----------分割线start")
            print("   execute[{0}]语句:\n{1}".format(index, sql))

            print('execute error execute:')
            content = repr(e)
            error_content = StringHelper.error(content)
            print(error_content)

            conn.rollback()
            print("分割线end--------我分割我分割----------分割线end")
            conn.commit()
            self.close(conn, cursor)
            raise Exception("数据execute失败")

    def execute_sqls(self, sql, params=None):
        sqllist = sql.split(";")
        for index, sql in enumerate(sqllist):
            if sql.strip():
                sql = self.sql_clean(sql)
                self.execute(sql, index=index)

    def sql_to_table(self, tb, to_db_conn, sql=None, sql_filename=None, if_exists="append"):

        mysqlinstance_to = mysqlhelper(**to_db_conn)

        if sql_filename:
            df = self.get_df_loadfile(sql_filename)
        else:
            sql = self.sql_clean(sql)
            df = self.get_df(sql)
            mysqlinstance_to.insertmany_bydf(df, tb, if_exists=if_exists)

    @try_rerun(dingding=True, n=6, sleep_time=5)
    def getconn(self):
        if self.pools:
            conn = self.dbinstance.getconn(self.host, self.port, self.user, self.passwd, self.db, self.charset)
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

    def sql_clean(self, sql):

        sql = re.sub("\,\s*\)", ")", sql)
        sql = re.sub("\.0,", ",", sql)

        sql = sql.replace(", nan", "").replace("nan, ", "").replace("None, ", "").replace(", None", "")

        return sql

    def creat_tb_from_tb(self, from_tb, to_tb, to_db=None):

        from_tb_basename = from_tb

        if "." in from_tb:
            from_tb_basename = from_tb.split(".")[1]

        sql = "show create table {0};".format(from_tb)

        df = self.get_df(sql)

        create_table = "DROP TABLE IF EXISTS {0};".format(to_tb)

        create_table = create_table + df["Create Table"].values[0].replace(
            "TABLE `" + from_tb_basename + "`", "TABLE " + to_tb
        )

        mysqlinstance = mysqlhelper(**to_db)

        mysqlinstance.execute_sqls(create_table)

        df = self.get_df("select * from {0};".format(from_tb))

        mysqlinstance.insertmany_bydf(df, to_tb, if_exists="append")
