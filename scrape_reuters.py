import os
import urllib.request, urllib.error, urllib.parse
import re
import datetime
import configparser
import json
import argparse
import pymysql


from bs4 import BeautifulSoup#これとsoup.で引っ張ってきている
from http.cookiejar import Cookie, CookieJar
from urllib.parse import urlparse, urljoin

# f_root_url = "http://jp.reuters.com/"
# f_start_url="http://jp.reuters.com/"
# f_max_depth=1000
# f_allowed_domains=["http://jp.reuters.com"]


parser = argparse.ArgumentParser()
parser.add_argument("--debug", type=bool, default=False)
args = parser.parse_args()

con = pymysql.connect( host='localhost', user='root', password='', db='scrape', charset='utf8', cursorclass=pymysql.cursors.DictCursor)
cur = con.cursor()





def get_conf(conf_path):
    config = configparser.ConfigParser()
    config.read(conf_path)

    return config


def get_html(url):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0")
    html = urllib.request.urlopen(req)

    soup = BeautifulSoup(html, 'html.parser')
    return soup


class Reuters:
    def __init__(self, conf_path='./config.ini'):
        config = get_conf(conf_path)
        self.root_url = config.get('rules', 'root_url')
        self.start_url = config.get('rules', 'start_url')
        self.allowed_domains = json.loads(config.get('rules', 'allowed_domains'))
        self.max_depth = int(config.get('rules', 'max_depth'))

        self.display_config()

        self.visited_url_list = set()



    def display_config(self):
        print("------scraper config ------")
        print('start_url: ', self.start_url)
        print('allowed_domains: ', self.allowed_domains)
        print('max_depth: ', self.max_depth)
        print("--------------------\n")

    def extract_urls(self, soup):
        result = set()
        for x in soup.find_all("a"):
            url = urljoin(self.root_url, x.get("href"))
            domain = urlparse(url).netloc
            if url is not None:
                if url not in self.visited_url_list:
                    self.visited_url_list.add(url)
                    if domain in self.allowed_domains:#ここ入ってない
                        result.add(url)

        return result

    def insert_db(self, url, date, title, content):
        check_sql = "select count(id) from scrape.reuters where title=\'{}\'".format(title)
        cur.execute(str(check_sql))
        cnt = cur.fetchall()
        if cnt.count(id) == 0:
            sql = """insert into scrape.reuters (url, date, title, content) values
                     (\'{}\', \'{}\', \'{}\', \'{}\')""".format(url, date, title, content)

            cur.execute(sql)
            con.commit()

    def recursive(self, url, depth):
        if depth > self.max_depth:
            return "reach max depth"

        try:
            soup = get_html(url)#ここで失敗してる
            print(self.root_url)
        except urllib.error.HTTPError as e:
            print("!error {}: cant get html from {}, skip this".format(e.code, url))
            return -1
        if re.search("article/", url) is not None:#ここ
            print(url)
            # extract date
            date=soup.find('span',class_="timestamp").text
            print(date)
            date_list = [int(x) for x in re.sub("[^\d]"," ", date.strip()).split()]
            print(date_list)

            # extract title
            title=soup.find('title').text
            # extract content
            content_candidates =soup.find("span",id="articleText").find_all("p")

            content = "".join([x.text for x in content_candidates])


            try:
                self.insert_db(url, insert_date, title, content)
            except UnicodeEncodeError as e:
                print(e)

            print("visit: ", url)
            if args.debug:
                print("****** find article ******")
                print("url: ", url, "\n")
                print("date: ", date, "\n")
                print("title: ", title, "\n")
                print("content: ", content, "\n")
                print("**************************\n")

        all_urls = self.extract_urls(soup)
        print("b")
        if args.debug:
            print("next url count: ", len(all_urls))
            print("visited: ", len(self.visited_url_list))

        for next_url in all_urls:
            self.recursive(next_url, depth + 1)

    def scrape(self):
        print("------ start scraping ------")
        self.recursive(self.start_url, 0)
        print("------ end scraping ------")


if __name__ == '__main__':
    # now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    file_root = os.path.dirname(os.path.abspath(__file__))
    scraper = Reuters(conf_path=os.path.join(file_root, 'config.ini'))
    scraper.scrape()

    cur.close()
    con.close()
