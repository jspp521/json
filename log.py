import logging
import os


def mylog():
    """
    用于打印运行情况及输出日志
    :return: logger对象
    """
    # 设置log文件保存位置及格式
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename=os.path.dirname(__file__) + '/crawl.log', filemode='w')

    # 创建Logger并设置级别
    logger = logging.getLogger('crawler')
    logger.setLevel(logging.DEBUG)

    # 创建handler并设置级别
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # 设置输出格式
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    # 将handle添加至logger,同时避免创建多个handler
    if not logger.handlers:
        logger.addHandler(ch)
        return logger
    else:
        return logger

