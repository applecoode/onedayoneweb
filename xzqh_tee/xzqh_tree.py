import requests
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from multiprocessing import Pool
client = MongoClient()
mongodb = client.rabbit.tjj_xzqh
re_url = re.compile(r'(^.*/)\w+.html$')

def download_url(url):#个人认为是完美的链接函数
    head = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8','Accept-Encoding':'gzip, deflate','Accept-Language':'zh-CN,zh;q=0.9','Cookie':'_trs_uv=ja9ept4l_6_wzl; AD_RS_COOKIE=20080919','Host':'www.stats.gov.cn','Proxy-Connection':'keep-alive','Upgrade-Insecure-Requests':'1','User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
    r = 0
    for i in range(3):
        print('链接' + url)
        try:
            r = requests.get(url,headers = head,timeout=3)
            if r.status_code == 200:
                break
            else:
                print('错误'+ str(r.status_code))
        except:
            print('链接超时')
            time.sleep(3)
    if r == 0:#网页错误标识
        with open('error.log','w') as f:
            f.writelines(url)
        return r
    r.encoding = r.apparent_encoding
    return r.text

def parsle_url(content):
    if content == 0:
        return content
    soup = BeautifulSoup(content,'lxml')
    return soup

def insert_mongo(list):#链接数据库
    print('插入数据库')
    mongodb.insert_many(list)
    print('插入完成')

def catgroy_xzqh(url):#按省市县分类
    citycontent = download_url(url)
    soup_city = parsle_url(citycontent)
    if soup_city == 0:
        return
    try:
        if soup_city.find(class_= 'provincetr'):
            xzqh_list = soup_city.find(class_= 'provincetr').parent.find_all('a')
            flag = 0#传递给下一个解析函数
        elif soup_city.find(class_= 'citytr'):
            xzqh_list = soup_city.find(class_='citytr').parent.find_all('a')
            flag = 1
        elif soup_city.find(class_= 'countytr'):
            xzqh_list = soup_city.find(class_='countytr').parent.find_all('a')
            flag = 1
        elif soup_city.find(class_= 'towntr'):
            xzqh_list = soup_city.find(class_='towntr').parent.find_all('a')
            flag =1
        else:
            xzqh_list = soup_city.find('tr',class_='villagetr').parent.find_all('td',class_= '')
            flag = 2
    except:
        print('这里木有你想要的东西')
        return
    return xzqh_list,url,flag


def get_url(list,url,flag):#正式获取函数
    xzqh_list_url = []
    xzqh_list_content = []
    url = re_url.match(url).group(1)
    if flag == 0:
        for i in list:
            xzqh_list_url.append(url+i['href'])
            xzqh_dict = {}
            xzqh_dict['year'] = '2014'#年份
            xzqh_dict['name'] = i.text
            xzqh_dict['code'] = i['href'][-7:-5]+'0000'
            xzqh_list_content.append(xzqh_dict)
        insert_mongo(xzqh_list_content)
    elif flag == 1:
        for i in range(len(list)//2):
            xzqh_list_url.append(url+list[i*2]['href'])
            xzqh_dict={}
            xzqh_dict['year'] = '2014'#年份
            xzqh_dict['name'] = list[i*2+1].text
            xzqh_dict['code'] = list[i*2].text#bijiao
            xzqh_list_content.append(xzqh_dict)
        insert_mongo(xzqh_list_content)
    else:
        for i in range(len(list)//3):
            print('处理'+list[i*3+2].text)
            xzqh_dict={}
            xzqh_dict['year'] = '2014'#年份
            xzqh_dict['name'] = list[i*3+2].text
            xzqh_dict['cxfl'] = list[i*3+1].text
            xzqh_dict['code'] = list[i*3].text#bijiao
            xzqh_list_content.append(xzqh_dict)
        insert_mongo(xzqh_list_content)
        return      #叶子节点
    return xzqh_list_url

def lets_go(url):   #第n层递归
    cat_r = catgroy_xzqh(url)
    key = get_url(cat_r[0],cat_r[1],cat_r[2])
    if key == None:  #递归终止点
        return
    for i in key:
        lets_go(i)   #第n-1层递归
    print(url+'处理完成')

if __name__=='__main__':
    cat_r = catgroy_xzqh('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2014/index.html')  #处理首层获得足够url列表
    key = get_url(cat_r[0], cat_r[1], cat_r[2])
    p = Pool()                     #多线程模式
    for i in key:                  #多线程模式
        p.apply_async(lets_go,args=(i,))  #多线程模式
    p.close()                      #多线程模式
    p.join()                       #多线程模式
    print('全部处理完成')


# url2 = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/37/14/03/371403108.html'
# url3 = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/37.html'
# url4 = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/'
# city_list = []
# city_urllist = []
# for province in get_province(url1)[0]:
#     city_urllist.extend(get_city(province)[0])
#     city_list.extend(get_city(province)[1])
# county_list = []
# county_urllist = []
# for city in city_urllist:
#     county_urllist.extend(get_county(city)[0])
#     county_list.extend(get_county(city)[1])
# town_list = []
# town_urllist =[]
# for county in county_urllist:
#     town_urllist.extend(get_town(county)[0])
#     town_list.extend(get_town(county)[1])
# s_list = get_xzqh(url1)



# def get_province(url):
#     provinceurl = download_url(url)
#     soup_province = parsle_url(provinceurl)
#     province_list = soup_province.find(class_= 'provincetr').parent.find_all('a')
#     province_list_url = []
#     province_list_content = []
#     for i in province_list:
#         province_list_url.append(url1+i['href'])
#         province_dict={}
#         province_dict['name'] = i.text
#         province_dict['code'] = i['href'][:2]+'0000'
#         province_list_content.append(province_dict)
#     return province_list_url,province_list_content
#
# def get_city(url):
#     citycontent = download_url(url)
#     soup_city = parsle_url(citycontent)
#     city_list = soup_city.find(class_= 'citytr').parent.find_all('a')
#     city_list_url = []
#     city_list_content = []
#     for i in range(len(city_list)//2):
#         city_list_url.append(url[:-7]+city_list[i*2]['href'])
#         city_dict={}
#         city_dict['name'] = city_list[i*2+1].text
#         city_dict['code'] = city_list[i*2]['href'][:2]+'00'
#         city_list_content.append(city_dict)
#     return city_list_url,city_list_content
#
# def get_county(url):
#     print('接受'+url)
#     citycontent = download_url(url)
#     soup_city = parsle_url(citycontent)
#     try:
#         city_list = soup_city.find(class_= 'countytr').parent.find_all('a')#bijiao
#     except:
#         print('这里没有县')
#         return [],[]
#     city_list_url = []
#     city_list_content = []
#     for i in range(len(city_list)//2):
#         print('正在处理'+city_list[i*2+1].text)
#         city_list_url.append(url[:-9]+city_list[i*2]['href'])#bijiao
#         city_dict={}
#         city_dict['name'] = city_list[i*2+1].text
#         city_dict['code'] = city_list[i*2].text#bijiao
#         city_list_content.append(city_dict)
#     return city_list_url,city_list_content
#
# def get_town(url):
#     print('接受'+url)
#     citycontent = download_url(url)
#     soup_city = parsle_url(citycontent)
#     try:
#         city_list = soup_city.find(class_= 'towntr').parent.find_all('a')#bijiao
#     except:
#         print('这里没有镇')
#         return [],[]
#     city_list_url = []
#     city_list_content = []
#     for i in range(len(city_list)//2):
#         print('正在处理'+city_list[i*2+1].text)
#         city_list_url.append(url[:-11]+city_list[i*2]['href'])
#         city_dict={}
#         city_dict['name'] = city_list[i*2+1].text
#         city_dict['code'] = city_list[i*2].text#bijiao
#         city_list_content.append(city_dict)
#     return city_list_url,city_list_content
