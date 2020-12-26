#!/usr/bin/env python
# coding: utf-8

import requests
from Logger import logger
from urllib.parse import quote
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, DateTime, DECIMAL, Table, MetaData, UniqueConstraint, ForeignKey
from sqlalchemy.dialects.mysql import LONGTEXT
from datetime import datetime, date, timedelta
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.mysql import insert
import time
import random
import re
import configparser
from fake_useragent import UserAgent


class CConfg:
    def __init__(self, name):
        self._name = name
        self._db = ""
        self._start = ""
        self._end = ""

        # 读取配置文件
        cfg = configparser.ConfigParser()
        filename = cfg.read(filenames=self._name)
        if not filename:
            raise Exception('配置文件不存在，请检查后重启!')

        self._db = cfg.get('GLOBAL', 'db')
        try:
            self._start = cfg.get('SCHEDULE', 'start')
            self._end = cfg.get('SCHEDULE', 'end')
            logger.info("计划任务{}~{}".format(self._start, self._end))
        except:
            logger.info('未配置计划任务')

    @property
    def db(self):
        return self._db

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end


cConfg = CConfg('config.ini')
engine = create_engine(cConfg.db, encoding='utf-8', echo=True)
conn = engine.connect()

metadata = MetaData()
t_bid = Table('house', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('title', Text()),
              Column('loupan', Text()),
              Column('house_type', Text()),
              Column('area', Text()),
              Column('toward', Text()),
              Column('renovation', Text()),
              Column('positionInfo', Text()),
              Column('totalPrice', Text()),
              Column('unitPrice', DateTime()),
              Column('href', String(255)),
              Column('createtime', DateTime()),
              UniqueConstraint('href', name='idx_href')
              )


class Bid(object):
    def __init__(self, title, loupan, house_type, area, toward, renovation, positionInfo, totalPrice, unitPrice, href):
        self.title = title
        self.loupan = loupan
        self.house_type = house_type
        self.area = area
        self.toward = toward
        self.renovation = renovation
        self.positionInfo = positionInfo
        self.totalPrice = totalPrice
        self.unitPrice = unitPrice
        self.href = href
        self.createtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bid_upsert(bid):
    insert_stmt = insert(t_bid).values(
        id=bid.id,
        title=bid.title,
        loupan=bid.loupan,
        house_type=bid.house_type,
        area=bid.area,
        toward=bid.toward,
        renovation=bid.renovation,
        positionInfo=bid.positionInfo,
        totalPrice=bid.totalPrice,
        unitPrice=bid.unitPrice,
        href=bid.href,
        createtime=bid.createtime)
    # print(insert_stmt)

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        title=insert_stmt.inserted.title,
        loupan=insert_stmt.inserted.loupan,
        house_type=insert_stmt.inserted.house_type,
        area=insert_stmt.inserted.area,
        toward=insert_stmt.inserted.toward,
        renovation=insert_stmt.inserted.renovation,
        positionInfo=insert_stmt.inserted.positionInfo,
        totalPrice=insert_stmt.inserted.totalPrice,
        unitPrice=insert_stmt.inserted.unitPrice,
        href=insert_stmt.inserted.href,
        createtime=insert_stmt.inserted.createtime,
        status='U')
    conn.execute(on_duplicate_key_stmt)


mapper(Bid, t_bid)
metadata.create_all(engine)


class LianjiaSpider:
    def __init__(self):
        pass

    def run_page(self, page):
        # 构建请求头
        ua = UserAgent()
        headers = {
            'user-agent': ua.Chrome
        }

        # 声明一个列表存储字典
        data_list = []

        url = 'https://sh.lianjia.com/ershoufang/pg{}/'.format(page)
        # 请求url
        try:
            resp = requests.get(url, headers=headers)
        # 讲返回体转换成Beautiful
        except requests.RequestException as e:
            logger.error(e)
        else:
            # print(resp.text)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'lxml')
                # 筛选全部的li标签
                sellListContent = soup.select('.sellListContent li.LOGCLICKDATA')
                # 循环遍历
                for sell in sellListContent:
                    try:
                        href = sell.select('a.LOGCLICKDATA')[0]['href']
                        print(href)
                        # 题目
                        title = sell.select('div.title a')[0].string
                        # 先抓取全部的div信息，再针对每一条进行提取
                        houseInfo = list(sell.select('div.houseInfo')[0].stripped_strings)
                        # 楼盘名字
                        loupan = houseInfo[0]
                        # 对剩下的信息进行分割
                        info = houseInfo[0].split('|')
                        # 房子类型
                        house_type = info[1].strip()
                        # 面积
                        area = info[2].strip()
                        # 朝向
                        toward = info[3].strip()
                        # 装修类型
                        renovation = info[4].strip()
                        # 地址
                        positionInfo = ''.join(list(sell.select('div.positionInfo')[0].stripped_strings))
                        # 总价
                        totalPrice = ''.join(list(sell.select('div.totalPrice')[0].stripped_strings))
                        # 单价
                        unitPrice = list(sell.select('div.unitPrice')[0].stripped_strings)[0]

                        # # 声明一个字典存储数据
                        # data_dict = {}
                        # data_dict['title'] = title
                        # data_dict['loupan'] = loupan
                        # data_dict['house_type'] = house_type
                        # data_dict['area'] = area
                        # data_dict['toward'] = toward
                        # data_dict['renovation'] = renovation
                        # data_dict['positionInfo'] = positionInfo
                        # data_dict['totalPrice'] = totalPrice
                        # data_dict['unitPrice'] = unitPrice
                        #
                        # data_list.append(data_dict)
                        # print(data_dict)
                        bid = Bid(title=title, loupan=loupan, house_type=house_type, area=area, toward=toward, renovation=renovation,
                                  positionInfo=positionInfo, totalPrice=totalPrice, unitPrice=unitPrice, href=href,)
                        bid_upsert(bid)

                    except Exception as e:
                        logger.error(e)
                        continue


if __name__ == '__main__':
    spider = LianjiaSpider()
    spider.run_page(100)
