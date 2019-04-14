import pymysql
from log import mylog


logger = mylog()


def insert_data(data, info=None):
    """
    插入数据进MySQL

    :param data: 要插入的数据
    :param info: 连接数据库的基本信息
    :return:
    """

    if len(info) != 6:
        return logger.error('传入的info长度有误！')

    db = pymysql.connect(host=info[0], user=info[1], password=info[2], port=info[3], db=info[4])
    cursor = db.cursor()

    # 将数据转化为字符串用于SQL语句
    table = info[5]
    keys = ', '.join(data.keys())
    values = ', '.join(['%s'] * len(data))
    sql = '''INSERT INTO {table}({keys}) VALUES ({values})'''.format(table=table, keys=keys, values=values)

    # 插入数据，出错则回滚
    try:
        if cursor.execute(sql, tuple(data.values())):
            logger.info('插入成功！')
            db.commit()
    except Exception as e:
        logger.error(e)
        db.rollback()
    db.close()
