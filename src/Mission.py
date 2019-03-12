#!/usr/bin/env python
# coding=utf-8

"""
@module  : sql_insert.py
@author  : Rinne
@contact : yejunbin123@qq.com
@time    : 2019 / 01 / 22
"""

from src import insert_information
import time
import datetime
import logging


class Mission:
    def __init__(self):
        self.do_mission = insert_information.InsertInformation()
        self.hour = 21

        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.INFO)
        self.handler = logging.FileHandler("log.txt")
        self.formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(process)d %(threadName)s '
                                           '%(filename)s lINE:%(lineno)d %(funcName)s %(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

    def mission(self):
        time_start = time.time()
        self.do_mission.insert()
        time_end = time.time()
        print('time cost', time_end - time_start, 's')

    def star_mission(self):
        while True:
            now = datetime.datetime.now()

            if now.hour == self.hour:
                self.mission()
            else:
                time.sleep(3600)
