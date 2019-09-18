import requests
import json
import re
import threading
import time
import random
from requests import Session
from queue import Queue, Empty
from lxml import etree

COOKIES = "save_jy_login_name=13066882860; myuid=79690859; is_searchv2=1; stadate1=79690859; myage=32; mysex=m; myincome=20; user_attr=000000; accessID=2019072216471727439; myloc=71%7C7101; upt=Oq6YxLpLpCY8D-qp6anlvjJyRwbd5JXktAIV4VyfOHQCNYNHMH-9a-BLPiOQjn4WgSfu7667pcme%2A9Nc3kWBM9MM4Q..; PHPSESSID=16a44fad94ab9cbcb5ef157d628f92f4; jy_refer=sp0.baidu.com; ip_loc=44; main_search:80690859=%7C%7C%7C00; user_access=1; PROFILE=80690859%3Axxxxx%3Am%3Aat3.jyimg.com%2Fa0%2Fee%2Ff60621ced9a79486ccce25851f2c%3A1%3A%3A1%3Af60621ced_1_avatar_p.jpg%3A1%3A1%3A50%3A0%3A3.0; COMMON_HASH=a0f60621ced9a79486ccce25851f2cee; last_login_time=1565062689; SESSION_HASH=2e19c868118350f05a27d4b73da22c318241eed8; FROM_BD_WD=%25E4%25B8%2596%25E7%25BA%25AA%25E4%25BD%25B3%25E7%25BC%2598; FROM_ST_ID=1764229; FROM_ST=.jiayuan.com; RAW_HASH=QURdgbOa2Mtasay5bFrpmVMzzL1CrW7JQ%2A4lV9eO8L7dOR7yyMWuRIBTYywWuGbzcfcIqaYqhjz3IINQIC0IJUr0rR2GnMOu3n7-ATYAoNWEAQ8.; pop_time=1565094770411"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
}

form_data = {
    "sex": "f",
    "key": "",
    "stc":	"1:43,2:20.35,3:153.170,23:1",
    "sn": "default",
    "sv": "1",
    "p": "1",
    "f": "select",
    "listStyle": "bigPhoto",
    "pri_uid": "80690859",
    "jsversion": "v5",
}

proxies = [
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.171:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.135:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.122:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.111:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.99:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.88:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.77:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.66:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.55:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.44:65000"},
    {"http": "http://ldj201986yt:ldj201986yt@218.87.74.33:65000"},
]


class Jiayuan(Session):
    def get_str(self, url, **kwargs):
        resp = super(Jiayuan, self).get(url, **kwargs)
        return resp.content.decode()

    def post_str(self, url, data=None, json=None, **kwargs):
        resp = super(Jiayuan, self).post(url, data=data, json=json, **kwargs)
        return resp.content.decode()

    def cookies_to_dict(self, cookies):
        return {cookie.split("=")[0]: cookie.split("=")[1] for cookie in cookies.split("; ")}

    def add_cookie_to_session(self, cookies):
        if isinstance(cookies, str):
            cookie_dict = self.cookies_to_dict(cookies)
        elif isinstance(cookies, dict):
            cookie_dict = cookies
        else:
            raise requests.ConnectionError
        requests.utils.add_dict_to_cookiejar(self.cookies, cookie_dict)


class CrawlGirlId(threading.Thread):
    session = None
    user_id_queue = None
    page_queue = Queue()
    POST_SEARCH_URL = "http://search.jiayuan.com/v2/search_v2.php"

    def __init__(self, formdata):
        super(CrawlGirlId, self).__init__()
        self.formdata = formdata.copy()  # 浅复制，因为每个线程都需要修改formdata["p"]，如果共享同一个的话会出乱子

    def run(self):
        global craw_pages_finished
        while True:
            try:
                page = str(self.page_queue.get(False))  # false，在队列为空时产生Empty异常
                self.formdata["p"] = page
            except Empty:
                craw_pages_finished = True  # 爬取所有页面的id号结束
                print("craw pages finished: %s" % threading.current_thread())
                break
            else:
                proxy = random.choice(proxies)
                try:
                    page_resp = self.session.post_str(url=self.POST_SEARCH_URL, data=self.formdata, proxies=proxy)
                except ConnectionError:
                    with global_lock:
                        proxies.remove(proxy)  # 说明这个代理无效或被封，移除
                else:
                    self.parse_girl_id(page_resp)
                    time.sleep(0.5)

    def parse_girl_id(self, page_resp):
        ret = re.sub(r"##jiayser##/{0,2}", "", page_resp)
        ret_dict = json.loads(ret)
        userinfo = ret_dict["userInfo"]
        for i in range(0, len(userinfo)):
            uid = userinfo[i]["realUid"]
            self.uid_queue.put(uid)  # block 默认为True，即如果队列满了则阻塞至队列有空位

    @classmethod
    def init_spider(cls, session, formdata, uid_queue):
        cls.uid_queue = uid_queue
        cls.session = session
        while True:
            if cls.get_first_page(formdata):
                break

    @classmethod
    def get_first_page(cls, formdata):
        formdata["p"] = "1"
        proxy = random.choice(proxies)
        try:
            resp = cls.session.post_str(url=cls.POST_SEARCH_URL, data=formdata, proxies=proxy)
        except ConnectionError:
            with global_lock:
                proxies.remove(proxy)  # 说明这个代理无效或被封，移除
            return False
        else:
            ret = re.sub(r"##jiayser##/{0,2}", "", resp)
            ret_dict = json.loads(ret)

            userinfo = ret_dict["userInfo"]
            for i in range(1, len(userinfo)):  # 第一个是本人信息，剔除
                uid = userinfo[i]["realUid"]
                cls.uid_queue.put(uid)  # block 默认为True，即如果队列满了则阻塞至队列有空位

            page_total = int(ret_dict["pageTotal"]) - 5  # 减去5是防止页面总数在爬取过程中减少，所以这个值动态获取其实更好
            count = ret_dict["count"]
            print("page_total: %s, count: %s" % (page_total, count))
            for page in range(2, page_total):  # 从第二页开始，第一页已经获取了
                cls.page_queue.put(page)
            return True


class CrawlDetailPage(threading.Thread):
    DETAIL_URL = "http://www.jiayuan.com/%s?fxly=search_v2"
    lock = threading.Lock()
    error_lock = threading.Lock()

    def __init__(self, user_id_queque, session, f, t_name):
        super(CrawlDetailPage, self).__init__(name=t_name)
        self.user_id_queque = user_id_queque  # 往url队列贴了个标签而已，实际上并不会新建一个队列，所以内存占用不会增加
        self.session = session
        self.fp = f
        self.item = {}

    def run(self):
        global craw_finished
        while not craw_finished:
            try:
                user_id = self.user_id_queque.get(True, timeout=0.5)
            except Empty:
                if craw_pages_finished:
                    craw_finished = True
                print("id empty: %s" % threading.current_thread())
            else:
                print("%s 正在爬取id=%s小姐姐资料" % (threading.current_thread(), user_id))
                detail_url = self.DETAIL_URL % user_id
                proxy = random.choice(proxies)
                try:
                    resp = self.session.get_str(url=detail_url, proxies=proxy)
                except ConnectionError:
                    with global_lock:
                        proxies.remove(proxy)  # 说明这个代理无效或被封，移除
                else:
                    self.item = {
                        "user_id": user_id,
                    }
                    self.parse_detail(resp)
                    time.sleep(0.5)
                    # print("id=%s小姐姐资料爬取完毕" % user_id)
        print("exit craw: %s" % threading.current_thread())

    def parse_detail(self, resp):
        try:
            html_element = etree.HTML(resp)  # 解析网页代码
            member_main_info = html_element.xpath("//div[@class='member_info_r yh']")[0]
            nickname = member_main_info.xpath("//div[@class='member_info_r yh']/h4/text()")[0]

            main_info = member_main_info.xpath("//div[@class='member_info_r yh']/h6/text()")[0]
            age = main_info.split("岁")[0]
            marriage = main_info.split("，")[1]  # 注意这里是中文的逗号

            base_info = member_main_info.xpath("//div[@class='member_info_r yh']//div[@class='fl pr']//text()")
            degree = base_info[1]
            height = base_info[4]
            # cars = base_info[7]
            salary = base_info[10]
            house = base_info[13]
            weight = base_info[16]
            # nation = base_info[22]

            other_info = html_element.xpath("//div[@class='content_705']/div[9]")[0]
            hometown = other_info.xpath(".//ul[1]/li[1]/div//text()")[0]

            requirment = html_element.xpath("//div[@class='content_705']/div[5]//li//div/text()")
            age_boy = requirment[0]
            height_boy = requirment[1]
            degree_boy = requirment[3]
            marriage_boy = requirment[5]
            hometown_boy = requirment[6]

            item_info = {
                "nickname": nickname,
                "age": age,
                "marriage": marriage,
                "degree": degree,
                "height": height,
                # "cars": cars,
                "salary": salary,
                "house": house,
                "weight": weight,
                # "nation": nation,
                "hometown": hometown,
                "age_boy": age_boy,
                "height_boy": height_boy,
                "degree_boy": degree_boy,
                "marriage_boy": marriage_boy,
                "hometown_boy": hometown_boy,
            }
            self.item.update(item_info)
            with self.lock:
                self.fp.write((json.dumps(self.item, ensure_ascii=False) + "\n"))
        except Exception as e:  # 记录错误信息
            with self.error_lock:
                with open("error.txt", "a", encoding="utf8") as fp:
                    fp.write(str(e) + "\r\n" + resp + "\r\n\r\n")


def main():
    user_id_queue = Queue()
    fp = open("girls_153-170.json", "a", encoding="utf8")

    jiayuan = Jiayuan()
    jiayuan.add_cookie_to_session(COOKIES)
    jiayuan.headers.update()

    CrawlGirlId.init_spider(jiayuan, form_data, user_id_queue)

    print("***** 爬虫开始 *****")
    start = time.time()

    craw_id_thread = []
    for loop in range(0, 1):
        crawgirlid = CrawlGirlId(form_data)
        crawgirlid.start()
        craw_id_thread.append(crawgirlid)

    detail_thread = []
    for loop in range(0, 10):
        thread_name = "thread" + str(loop)
        detail = CrawlDetailPage(user_id_queue, jiayuan, fp, thread_name)
        detail.start()
        detail_thread.append(detail)

    for thread in craw_id_thread:
        thread.join()

    for thread in detail_thread:
        thread.join()

    fp.close()
    end = time.time()
    print("***** 爬虫结束，总计耗时：%f秒 *****" % (end - start))


if __name__ == "__main__":
    craw_pages_finished = False  # 获取所有页面的id完毕
    craw_finished = False  # 所有详情抓取完毕，即爬虫结束
    global_lock = threading.Lock()
    main()
