import requests
import bs4
import re
bs = bs4.BeautifulSoup

def get_xzqh():
    pass

def get_url(url):
    r = s.get(url)
    r.encoding = r.apparent_encoding  # 清除汉字乱码
    b1 = bs(r.text, 'lxml')
    html_a_list = b1.find('a').parent.parent.parent.find_all('a')
    print('找到标签' + b1.find('a').text)
    xzqh_list = []
    for i in range(len(html_a_list)):
        if re.match(r'.*?/\d{6}.html',html_a_list[0]['href']):
            url_middle = html_a_list[0]['href'][3:5] + '/'
        elif re.match(r'.*?/\d{9}.html',html_a_list[0]['href']):
            url_middle = html_a_list[0]['href'][3:5] + '/'+ html_a_list[0]['href'][5:7] + '/'
        else: url_middle = ''
        print('这里的href = ' + url_middle + html_a_list[i]['href'])
        url_list.add(root_url + url_middle+ html_a_list[i]['href'])
    for i in range(len(html_a_list) // 2):
        xzqh_dict = {}
        xzqh_dict['xzqh_dm'] = html_a_list[i * 2].text
        xzqh_dict['xzqh_mc'] = html_a_list[i * 2 + 1].text
        xzqh_list.append(xzqh_dict)
    for tem_url in url_list:
        print('进入'+ tem_url)
        url_list.add(get_url(tem_url)[0])
        print('更新'+ tem_url)
    return url_list

s = requests.Session()
head = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8','Accept-Encoding':'gzip, deflate','Accept-Language':'zh-CN,zh;q=0.9','Cookie':'_trs_uv=ja9ept4l_6_wzl; AD_RS_COOKIE=20080919','Host':'www.stats.gov.cn','Proxy-Connection':'keep-alive','Upgrade-Insecure-Requests':'1','User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
s.headers.update(head)
root_url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/'
# url_listr,xzqh_listr= get_url(root_url)
url_listr= get_url('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/37/3714.html')
