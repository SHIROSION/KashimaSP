#!/usr/bin/env python
# coding=utf-8

"""
@module  : SQLConnect.py
@author  : Rinne
@contact : yejunbin123@qq.com
@time    : 2019 / 01 / 22
"""

import tushare as ts
from src import SQLConnect
import time
import logging

class InsertInformation:
    """ save stock data and read data.

    this class main to use api save stock data and read data.

    Attributes:
    """
    def __init__(self):

        self.logger = logging.getLogger()

        # set api token
        ts.set_token("c1a2905ed03d90b822a60c04c63093e35953ee60ae811be117948546")

        # request api to get data
        self.pro = ts.pro_api()
        data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

        try:
            # save stock information data
            data.to_csv('D://python project/teststock/data/stock_information.csv')
            self.logger.info("stock information save successful")
        except IOError:
            self.logger.error("stock information save Error")

        self.stock_information = 'D://python project/teststock/data/stock_information.csv'

        try:
            self.stock_file = open(self.stock_information, 'r', encoding="UTF-8")
        except IOError:
            self.logger.error("open stock file Error")

        self.use_sql = SQLConnect.SQLConnect()

    def insert(self):
        """ save stock data and read data to insert data.

        this method is main to read data to insert data to databases.

        :return:
        """

        # get_count: calculate the number of requests API.
        get_count = 0

        # st_info_count: calculate the stock information file rows.
        st_info_count = 0

        # stock_information_list: save stock information data.
        stock_information_list = []

        # read stock information data.
        # extract the stock code for each line read.
        # use stock code to request api to get data and save.
        # read data and insert data to databases.
        while True:
            # stock_data_list: save stock data.
            # financial_statements_information_list: save financial statements data.
            stock_data_list = []
            financial_statements_information_list = []

            information = self.stock_file.readline()
            get_stock_information = information.split(",")

            # delete stock information index.
            del get_stock_information[0]

            # calculate stock information file rows.
            for _ in enumerate(open(self.stock_information, 'r', encoding="UTF-8")):
                st_info_count += 1

            if len(information) <= 0:
                self.use_sql.insert_stock_information(stock_information_list, st_info_count - 1)
                self.stock_file.close()
                break

            elif get_stock_information[0] == 'ts_code':
                continue

            else:
                ts_code = ""
                stock_information_list.append(tuple(get_stock_information))
                get_ts_code = get_stock_information[0].split(".")

                # construct stock code strings.
                get_ts_code.insert(1, "_")

                for x in get_ts_code:
                    ts_code += x

                # request api to get data.
                df = self.pro.query('daily', ts_code="%s" % get_stock_information[0], start_date='', end_date='')
                fs = self.pro.query('income', ts_code="%s" % get_stock_information[0], start_date='', end_date='')

                data_file_path = "D://python project/teststock/data/%s.csv" % ts_code
                fs_data_file_path = "D://python project/teststock/data/%s_fs.csv" % ts_code

                # save data.
                df.to_csv(data_file_path)
                fs.to_csv(fs_data_file_path)

                open_data_file = open(data_file_path, 'r', encoding="UTF-8")
                open_fs_data_file = open(fs_data_file_path, "r", encoding="UTF-8")

                count = 0
                fs_count = 0
                fs_index_count = 1

                # calculate stock data file rows.
                for _ in enumerate(open(data_file_path, 'r')):
                    count += 1

                # calculate stock financial statements data file rows.
                for _ in enumerate(open(fs_data_file_path, 'r')):
                    fs_count += 1

                # read stock data file and insert data to database.
                while True:
                    data = open_data_file.readline()
                    stock_data = data.split(",")

                    # if the file is read out will insert data to database.
                    if len(data) <= 0:
                        self.use_sql.insert_stock_data(ts_code, stock_data_list, count - 1)
                        open_data_file.close()
                        break

                    elif stock_data[0] == "ts_code":
                        continue

                    else:
                        # delete stock data index.
                        del stock_data[0]
                        # delete stock code.
                        del stock_data[0]
                        stock_tuple = tuple(stock_data)
                        stock_data_list.append(stock_tuple)

                # read stock financial statements data file and insert data to database.
                while True:
                    fs = open_fs_data_file.readline()
                    fs_data = fs.split(",")

                    if len(fs) <= 0:
                        self.use_sql.insert_stock_financial_statements("%s_fs" % ts_code,
                                                                       financial_statements_information_list,
                                                                       fs_count - 1)
                        open_fs_data_file.close()
                        break

                    elif fs_data[0] == "":
                        continue

                    else:
                        # reordering index
                        fs_data[0] = fs_count - fs_index_count
                        fs_index_count += 1
                        fs_tuple = tuple(fs_data)
                        financial_statements_information_list.append(fs_tuple)

            # calculate the number of requests API.
            # if the request API reaches 200 times, it will sleep for 60s.
            if get_count == 200:
                time.sleep(60)
                get_count = 0

            else:
                get_count += 1
