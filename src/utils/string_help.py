import sys
import os


class StringHelp(object):

    @classmethod
    def str_norm(cls, mystr):
        mystr = mystr.replace("\\n", " ").replace("\\t", " ").replace("\\", " ")
        return mystr

    @classmethod
    def str_cut(cls, mystr):
        mystr = cls.str_norm(mystr)

        if len(mystr) > 600:
            mystr = mystr[:300] + '……' + mystr[-300:]

        return mystr

    @classmethod
    def error(cls, mystr):
        mystr = cls.str_cut(mystr)
        file_path = sys.argv[0]    # 获取文件路径和函数名
        file_name = file_path.split('/')[-1].split('.')[0]
        mystr = "报错信息：\n{0}\n执行脚本：\n{1}".format(mystr, file_name)

        return mystr
