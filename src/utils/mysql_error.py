import time
import functools
from config import config
from utils.string_help import StringHelp
from utils.dingding_robot import data_msg
from config import dingding_config


def get_sql_error(dingding=False):

    def my_decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                error_str = repr(e)
                error_content = StringHelp.error(error_str)
                func_name = func.__name__
                raise_content = '------------\n报错函数：\n{0}\n{1}\n---------------'.format(func_name, error_content)
                print(raise_content)

                if dingding:
                    data_msg(raise_content, dingding)
        return wrapper

    return my_decorator
