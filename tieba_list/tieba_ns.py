import requests
from bs4 import BeautifulSoup
import re
import time
url = 'https://tieba.baidu.com/f?ie=utf-8&kw=ns&fr=search'
def down_url(url):
    s = requests.session()
    for i in range(3):
        try:
            r = s.get(url)
            if r.status_code == 200:
                r.encoding = 'utf-8'
            else:
                print('error find:'+r.status_code)
        except:
            print('neterror,retrying')
            time.sleep(3)
    return r.text
def parse_url(url):
    rtext = down_url(url)
    bs = BeautifulSoup(rtext,'lxml')
    return bs
def get_url(soup):
    pass
def get_con(soup):
    pass
def lets_go(url):
    pass
