#-*- coding: utf-8 -*-
#Title : TencentVideo_bullet_crawler
#腾讯视频弹幕爬虫
#Author ： Richard
#Date: 2022/4/26

import pandas as pd
import time
import requests
from sqlalchemy import create_engine
# 配置链接数据库信息
db_config = {
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'filmproject_test',
    'username': 'root',
    'password': '1234'
}
# 数据库链接地址
db_url = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4'.format(**db_config)
# 创建数据库引擎
engine = create_engine(db_url)
# 创建数据库链接



def get_bullet(set_length,target_id,session_key,setname):

    headers = {
        'User-Agent': 'Googlebot'
    }
    # 初始为15，7245 为视频秒长，链接以三十秒递增\
    content = []
    upcount = []
    timepoint = []
    opername = []
    uservip = []
    page = []
    for i in range(15, int(set_length)*60, 30):
        nowtime = round(time.time() * 1000)
        print(round(i / (set_length*60) * 100, 2), '%')
        url = "https://mfm.video.qq.com/danmu?otype=json&target_id="+str(target_id)+"&session_key="+str(session_key)+"&timestamp=" + str(
            i) + "&_=" + str(nowtime)
        print(url)
        html = requests.get(url, headers=headers).json(strict=False)
        # print(html)
        time.sleep(1)
        for j in html['comments']:
            content.append(j['content'])

            upcount.append(j['upcount'])
            timepoint.append(j['timepoint'])
            opername.append(j['opername'])
            uservip.append(j['uservip_degree'])
            page.append(i)

        crawler_data = pd.DataFrame(
            {'content': content, 'upcount': upcount, 'timepoint': timepoint, 'opername': opername, 'uservip': uservip,
             'page': page})
        filename = '且试天下' + setname + 'bullet'

    crawler_data.to_sql(filename, engine)
    print("数据抓取&写入作业完毕！")

if __name__ == "__main__":
    t1 = time.time()
    set_length = 44 #该集时长(mins)
    target_id = '7771314486%26vid%3Dv00423206va'
    session_key = '0%2C228%2C1651040661'
    setname = 's01e02' #建议小写
    get_bullet(set_length=set_length,target_id=target_id,session_key=session_key,setname=setname)
    t2 = time.time()
    t3 = round(t2 - t1, 2)
    print('------本次抓取耗时:%s秒------' % t3)