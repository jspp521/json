import threading
import json
import time
import re
from requests import Request, Session
from queue import Queue
from log import mylog
from db import insert_data


logger = mylog()


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Fi',
}


def req_pool(url, url_num):
    """将数量从page到url_num之间的页面的待爬请求加入队列里等待调用"""

    # 设定初始页数、创建会话和队列
    page = 0
    s = Session()
    req_queue = Queue()

    while page < url_num:
        # 将传入的url转化为通用形式，并随着循环增加页数，每次循环生成新一页的url
        root_url = re.sub('(page=\d*)', 'page=%s', url)
        crwal_url = root_url % page
        page += 1

        # 创建Request对象，并设为待爬状态，加入队列中等待调用爬取
        req = Request('GET', crwal_url, headers=headers)
        prepped = s.prepare_request(req)
        req_queue.put(prepped)

    return req_queue


class JsonCrawler(threading.Thread):

    def __init__(self, que, info, field=None):

        # 继承threading以运行多线程
        threading.Thread.__init__(self)

        # 初始化会话和队列
        self.s = Session()
        self.que = que
        self.field = field
        self.info = info

    def run(self):
        """
        多线程运行的主程序
        """

        if self.field is None:
            self.field = []

        # 初始化单条数据对象，循环里用于插入数据库
        single_data = {}

        while True:

            # 从队列中取出一个请求
            req = self.que.get_nowait()

            # 用于判断队列是否为空，空则退出
            if req is None:
                break

            # 间隔0.5s执行爬取的请求，并解析出json数据集
            time.sleep(0.5)
            init_content = self.s.send(req)
            content = json.loads(init_content.content)
            logger.info(req.url)

            # 筛选需要的字段
            index = parse(self.field[0], content)[0]
            for i in content[index]:
                try:
                    for f in self.field:
                        single_data[f] = i[f]
                except TypeError:
                    logger.error("请传入列表格式的field")
                    break
                else:
                    insert_data(single_data, info=self.info)
                    # logger.info(single_data)
                    single_data = {}

            # 给队列添加空值，方便退出
            self.que.put(None)
        return None


def parse(k, data, index_list=None):
    """
    非爬虫部分，较为复杂，可跳过

    json通常嵌套了多个列表和字典循环，该方法用于寻找所需读取数据所在的层级
    目前适用于最简单常见的json格式，即数据集用列表存储，外部再嵌套一个大的字典
    形如{a:[{}, {}, ...{}], b: str, c: str}

    :param k: 需要寻找的数据的key
    :param data: 爬取到的json格式的数据
    :param index_list: 无需传入，按默认的None即可
    :return: 返回数据所在层级的索引值
    """

    if index_list is None:
        index_list = []

    # 格式效验
    if not isinstance(data, dict) and not isinstance(data, list):
        return logger.error('传入的数据格式有误，需为json格式。')

    # 如果对象是字典时所进行的递归
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict) and k in value.keys():
                return index_list
            elif isinstance(value, dict) or isinstance(value, list):
                index_list.append(key)
                return parse(k, value, index_list)

    # 如果对象是列表时所进行的递归
    if isinstance(data, list):
        for index, value in enumerate(data):
            if isinstance(value, dict) and k in value.keys():
                return index_list
            elif isinstance(value, dict) or isinstance(value, list):
                index_list.append(index)
                return parse(k, value, index_list)


def main():
    # 设置要爬取的URL数量和线程数
    URL_NUM = 5
    THREAD_NUM = 3

    # 设置要爬取的URL以及需要获得的数据的索引
    url = 'https://pacaio.match.qq.com/irs/rcd?cid=146&token=49cbb2154853ef1a74ff4e53723372ce&ext=digi&page=1'
    index_list = ['id', 'title', 'publish_time', 'source']

    # 设置接入数据库的信息
    # host、 username、 password、 port、 database、 table
    info = ['localhost', 'root', 'moledata123', 3306, 'weibo', 'test']

    # 执行主程序
    q = req_pool(url, URL_NUM)
    for i in range(THREAD_NUM):
        t = JsonCrawler(q, info, index_list)
        t.start()


if __name__ == '__main__':
    main()
