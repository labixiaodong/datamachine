from utils.mysql_help import MysqlHelper
from config import config
import pandas as pd


def data_insert():
    mysql_instance = MysqlHelper(**config.big_data)

    df = pd.read_excel('/Users/admin/Desktop/add_df.xlsx')
    mysql_instance.insertdata_bydf(df, 'add_test')
    print('插入成功！！！！！！！！！！')


def data_get():

    sql_1 = """
        select name, hp_max from heros
        order by hp_max desc limit 5
    """

    sql_2 = """
            select name, hp_growth from heros
            order by hp_growth desc limit 5
        """

    mysql_instance = MysqlHelper(**config.big_data)
    df1, df2 = mysql_instance.get_df(sql_1, sql_2)
    print(df1)
    print(df2)


def test():
    test_list = ['a', 'b', 'c']
    x, y, z = test_list
    print(x)
    print('*'*20)
    print(y)
    print('*' * 20)
    print(z)


if __name__ == '__main__':
    test()


