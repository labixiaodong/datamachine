from utils.mysql_help import MysqlHelper
from config import config
import pandas as pd


def main():
    mysql_instance = MysqlHelper(**config.big_data)

    df = pd.read_excel('/Users/admin/Desktop/add_df.xlsx')
    mysql_instance.insertdata_bydf(df, 'add_test')
    print('插入成功！！！！！！！！！！')


if __name__ == '__main__':
    main()


