
"""
数据库信息
"""

big_data = {}
big_data['host'] = '47.95.140.2'
big_data['port'] = 3306
big_data['user'] = 'root'
big_data['passwd'] = 'jxd200599'
big_data['db'] = 'wangzhe'


"""
本地数据库信息
"""
local_data = {}
local_data['host'] = '127.0.0.1'
local_data['port'] = 3306
local_data['user'] = 'root'
local_data['passwd'] = 'root1234'
local_data['db'] = 'wangzhe'

"""
线上数据库信息
"""

online_data = {}
online_data['host'] = '地址'
online_data['port'] = '端口'
online_data['user'] = '用户名'
online_data['passwd'] = '密码'
online_data['db'] = '数据库名称'

# 数据库连接编码
DB_CHARSET = "utf8"

# mincached : 启动时开启的闲置连接数量(缺省值 0 以为着开始时不创建连接)
DB_MIN_CACHED = 10

# maxcached : 连接池中允许的闲置的最多连接数量(缺省值 0 代表不闲置连接池大小)
DB_MAX_CACHED = 10

# maxshared : 共享连接数允许的最大数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
DB_MAX_SHARED = 10

# maxconnecyions : 创建连接池的最大数量(缺省值 0 代表不限制)
DB_MAX_CONNECYIONS = 20

# blocking : 设置在连接池达到最大数量时的行为(缺省值 0 或 False 代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
DB_BLOCKING = False

# axusage : 单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
DB_MAX_USAGE = 3

# setsession : 一个可选的SQL命令列表用于准备每个会话，如["set datestyle to german", ...]
DB_SET_SESSION = None
