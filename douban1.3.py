import requests
import threading
# import json
# from douban_parse import parse_url
from lxml import etree
from queue import Queue
from pymongo import MongoClient

from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 屏蔽 证书验证错误代码
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Douban(object):
    def __init__(self):
        self.client = MongoClient(host="192.168.1.60",port=27017)
        self.collection = self.client["douban"]["move"]
        self.start_url = "https://movie.douban.com/j/search_subjects?type=movie&tag={}&sort=recommend&page_limit=20&page_start={}"
        self.headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'zh-CN,zh;q=0.9',
          'Referer': 'https://movie.douban.com/explore',
          'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.96 Safari/537.36'}
        self.tag = ["热门","最新","经典","豆瓣高分"]
        self.t = (t for t in self.tag for _ in range(100))
        self.url_queue = Queue()
        self.json_queue = Queue()
        self.content_queue = Queue()
        self.save_queue = Queue()

    def get_json(self):
        while True:
            url = self.url_queue.get()
            html = requests.get(url,headers = self.headers,verify = False).json()
            subjects = html["subjects"]
            self.json_queue.put(subjects)
            self.url_queue.task_done()

    def introduction(self): #提取详情页数据
        # save_list = []
        while True:
            content_list = self.content_queue.get()
            for item in content_list:
                url = item.get("url")
                html = requests.get(url,headers = self.headers,verify = False)
                html_et = etree.HTML(html.text)
                item["director"] = html_et.xpath('//a[@rel="v:directedBy"]/text()')
                item["protagonist"] = html_et.xpath('//a[@rel="v:starring"]/text()')
                item["type"] = html_et.xpath('//span[@property="v:genre"]/text()')
                item["time"] = html_et.xpath('//span[@property="v:initialReleaseDate"]/text()')
                try:
                    item["info"] = html_et.xpath('//div[@id="link-report"]/span/text()[1]')[0].strip()
                except:
                    item["info"] = None
                item["class"] = next(self.t)
                print(item)
                self.collection.save(item)
            self.content_queue.task_done()

    def get_content_list(self): #提取数据
        content_list = []
        while True:
            html_json = self.json_queue.get()
            for i in html_json:
                item= {}
                item["_id"] = i.get("id")
                item["title"] = i.get("title")
                item["rate"] = i.get("rate")
                item["url"] = i.get("url")
                item["cover"] = i.get("cover")
                content_list.append(item)
            self.content_queue.put(content_list)
            self.json_queue.task_done()

    def get_url_list(self):
        
        url_list = [self.start_url.format(t,i*20) for t in self.tag for i in range(5)]
        for i in url_list:
            self.url_queue.put(i)

    def main(self):
        thread_list = []
        #1.url_list
        t_url = threading.Thread(target=self.get_url_list)
        thread_list.append(t_url)
        #2.遍历，发送请求，获取响应
        
        t_json = threading.Thread(target=self.get_json)
        thread_list.append(t_json)
        #3.提取数据
        
        t_html = threading.Thread(target=self.get_content_list)
        thread_list.append(t_html)

        #4.提取详情页数据
        
        t_info = threading.Thread(target=self.introduction)
        thread_list.append(t_info)
        #5.保存
        # t_save = threading.Thread(target=self.save_content_list)
        # thread_list.append(t_save)
        for t in thread_list:
            t.setDaemon(True) #把子线程设置为守护线程，该线程不重要主线程结束，子线程结束
            t.start()

        for q in [self.url_queue,self.json_queue,self.content_queue,self.save_queue]:
            q.join() #让主线程等待阻塞，等待队列的任务完成之后再完成



if __name__ == "__main__":
    douban = Douban()
    douban.main()