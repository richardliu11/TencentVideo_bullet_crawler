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


nowtime = round(time.time()*1000)
headers = {
    'User-Agent': 'Googlebot'
}
# 初始为15，7245 为视频秒长，链接以三十秒递增\
df = pd.DataFrame()
content=[]
upcount=[]
timepoint=[]
opername = []
uservip = []
page=[]
for i in range(15, 2640, 30):
    nowtime = round(time.time() * 1000)
    print(round(i/2640*100,2),'%')
    url = "https://mfm.video.qq.com/danmu?otype=json&target_id=7771314319%26vid%3Da0042ceiy5v&session_key=0%2C0%2C0&timestamp="+str(i)+"&_="+str(nowtime)
    print(url)
    html = requests.get(url, headers=headers).json()
    time.sleep(1)
    for j in html['comments']:
        content.append(j['content'])
        upcount.append(j['upcount'])
        timepoint.append(j['timepoint'])
        opername.append(j['opername'])
        uservip.append(j['uservip_degree'])
        page.append(i)


    crawler_data = pd.DataFrame(
        {'content': content, 'upcount':upcount,'timepoint':timepoint,'opername':opername,'uservip':uservip,'page':page})
    filename='且试天下'+'s01e01'+'bullet'
crawler_data.to_sql(filename, engine)
print("finish")
