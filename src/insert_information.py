#!/usr/bin/env python
# coding=utf-8

"""
@module  : SQLConnect.py
@author  : Rinne
@contact : yejunbin123@qq.com
@time    : 2019 / 01 / 22
"""

import csv
import datetime
import logging
import time

import tushare as ts

from src import SQLConnect


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
        self.years = 2000

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

        # calculate stock information file rows.
        for _ in enumerate(open(self.stock_information, 'r', encoding="UTF-8")):
            st_info_count += 1

        while self.years <= datetime.datetime.now().year:
            forecast_data_list = []
            fc_forecast_file_path = "D://python project/teststock/data/%s1231_fc.csv" % self.years
            fc = self.pro.query('forecast', period='%s1231' % self.years)
            fc.to_csv(fc_forecast_file_path)
            fc_count = 0
            reader = csv.reader(open(fc_forecast_file_path, "r", encoding="UTF-8"))

            # calculate forecast data file rows.
            for _ in enumerate(open(fc_forecast_file_path, 'r', encoding="UTF-8")):
                fc_count += 1

            for row in reader:
                if row[0] == "":
                    continue
                else:
                    forecast_data_list.append(tuple(row))

            self.use_sql.insert_forecast_data("%s1231_fc" % self.years, forecast_data_list, fc_count - 1)
            self.years += 1

        # read stock information data.
        # extract the stock code for each line read.
        # use stock code to request api to get data and save.
        # read data and insert data to databases.
        while True:
            # stock_data_list: save stock data.
            # financial_statements_information_list: save financial statements data.
            stock_data_list = []
            financial_statements_information_list = []
            debt_data_list = []
            cashflow_data_list = []
            dividend_data_list = []
            express_data_list = []
            fina_indicator_data_list = []

            information = self.stock_file.readline()
            get_stock_information = information.split(",")

            # delete stock information index.
            del get_stock_information[0]

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
                df = self.pro.query('daily', ts_code="%s" % get_stock_information[0])
                fs = self.pro.query('income', ts_code="%s" % get_stock_information[0])
                dbf = self.pro.query('balancesheet', ts_code="%s" % get_stock_information[0])
                cf = self.pro.query('cashflow', ts_code="%s" % get_stock_information[0])
                ddf = self.pro.query('dividend', ts_code="%s" % get_stock_information[0], start_date='', end_date='')
                ef = self.pro.query('express', ts_code="%s" % get_stock_information[0], start_date='', end_date='')
                fif = self.pro.query('fina_indicator', ts_code="%s" % get_stock_information[0], start_date='',
                                     end_date='', period='20181231')

                data_file_path = "D://python project/teststock/data/%s.csv" % ts_code
                fs_data_file_path = "D://python project/teststock/data/%s_fs.csv" % ts_code
                dbf_debt_file_path = "D://python project/teststock/data/%s_dbf.csv" % ts_code
                cf_cashflow_file_path = "D://python project/teststock/data/%s_cf.csv" % ts_code
                ddf_dividend_file_path = "D://python project/teststock/data/%s_ddf.csv" % ts_code
                ef_express_file_path = "D://python project/teststock/data/%s_ef.csv" % ts_code
                fif_fina_indicator_file_path = "D://python project/teststock/data/%s_fif.csv" % ts_code

                # save data.
                df.to_csv(data_file_path)
                fs.to_csv(fs_data_file_path)
                dbf.to_csv(dbf_debt_file_path)
                cf.to_csv(cf_cashflow_file_path)
                ddf.to_csv(ddf_dividend_file_path)
                ef.to_csv(ef_express_file_path)
                fif.to_csv(fif_fina_indicator_file_path)

                open_data_file = open(data_file_path, 'r', encoding="UTF-8")
                open_fs_data_file = open(fs_data_file_path, "r", encoding="UTF-8")
                open_dbf_debt_file = open(dbf_debt_file_path, "r", encoding="UTF-8")
                open_cf_cashflow_file = open(cf_cashflow_file_path, "r", encoding="UTF-8")
                open_ddf_dividend_file = open(ddf_dividend_file_path, "r", encoding="UTF-8")
                open_ef_express_file = open(ef_express_file_path, "r", encoding="UTF-8")
                open_fif_fina_indicator_file = open(fif_fina_indicator_file_path, "r", encoding="UTF-8")

                count = 0
                fs_count = 0
                dbf_count = 0
                cf_count = 0
                ddf_count = 0
                ef_count = 0
                fif_count = 0

                fs_index_count = 1
                dbf_index_count = 1
                cf_index_count = 1
                ddf_index_count = 1
                ef_index_count = 1
                fif_index_count = 1

                # calculate stock data file rows.
                for _ in enumerate(open(data_file_path, 'r', encoding="UTF-8")):
                    count += 1

                # calculate stock financial statements data file rows.
                for _ in enumerate(open(fs_data_file_path, 'r', encoding="UTF-8")):
                    fs_count += 1

                # calculate stock financial statements data file rows.
                for _ in enumerate(open(dbf_debt_file_path, 'r', encoding="UTF-8")):
                    dbf_count += 1

                # calculate cashflow data file rows.
                for _ in enumerate(open(cf_cashflow_file_path, 'r', encoding="UTF-8")):
                    cf_count += 1

                # calculate dividend data file rows.
                for _ in enumerate(open(ddf_dividend_file_path, 'r', encoding="UTF-8")):
                    ddf_count += 1

                # calculate express data file rows.
                for _ in enumerate(open(ef_express_file_path, 'r', encoding="UTF-8")):
                    ef_count += 1

                # calculate fina_indicator data file rows.
                for _ in enumerate(open(fif_fina_indicator_file_path, 'r', encoding="UTF-8")):
                    fif_count += 1

                # read stock data file and insert data to database.
                while True:
                    data = open_data_file.readline()
                    stock_data = data.split(",")

                    # if the file is read out will insert data to database.
                    if len(data) <= 0:
                        self.use_sql.insert_stock_data(ts_code, stock_data_list, count - 1)
                        open_data_file.close()
                        break

                    elif stock_data[0] == "":
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

                # read debt data file and insert data to database.
                while True:
                    dbf = open_dbf_debt_file.readline()
                    dbf_data = dbf.split(",")

                    if len(dbf) <= 0:
                        self.use_sql.insert_debt_data("%s_dbf" % ts_code, debt_data_list, dbf_count - 1)
                        open_dbf_debt_file.close()
                        break

                    elif dbf_data[0] == "":
                        continue

                    else:
                        # reordering index
                        dbf_data[0] = dbf_count - dbf_index_count
                        dbf_index_count += 1
                        dbf_tuple = tuple(dbf_data)
                        debt_data_list.append(dbf_tuple)

                # read cashflow data file and insert data to database.
                while True:
                    cf = open_cf_cashflow_file.readline()
                    cf_data = cf.split(",")

                    if len(cf) <= 0:
                        self.use_sql.insert_cashflow_data("%s_cf" % ts_code, cashflow_data_list, cf_count - 1)
                        open_cf_cashflow_file.close()
                        break

                    elif cf_data[0] == "":
                        continue

                    else:
                        # reordering index
                        cf_data[0] = cf_count - cf_index_count
                        cf_index_count += 1
                        cf_tuple = tuple(cf_data)
                        cashflow_data_list.append(cf_tuple)

                # read dividend data file and insert data to database.
                while True:
                    ddf = open_ddf_dividend_file.readline()
                    ddf_data = ddf.split(",")

                    if len(ddf) <= 0:
                        self.use_sql.insert_dividend_data("%s_ddf" % ts_code, dividend_data_list, ddf_count - 1)
                        open_ddf_dividend_file.close()
                        break

                    elif ddf_data[0] == "":
                        continue

                    else:
                        # reordering index
                        ddf_data[0] = ddf_count - ddf_index_count
                        ddf_index_count += 1
                        ddf_tuple = tuple(ddf_data)
                        dividend_data_list.append(ddf_tuple)

                # read express data file and insert data to database.
                while True:
                    ef = open_ef_express_file.readline()
                    ef_data = ef.split(",")

                    if len(ef) <= 0:
                        self.use_sql.insert_express_data("%s_ef" % ts_code, express_data_list, ef_count - 1)
                        open_ef_express_file.close()
                        break

                    elif ef_data[0] == "":
                        continue

                    else:
                        # reordering index
                        ef_data[0] = ef_count - ef_index_count
                        ef_index_count += 1
                        ef_tuple = tuple(ef_data)
                        express_data_list.append(ef_tuple)

                # read fina_indicator data file and insert data to database.
                while True:
                    fif = open_fif_fina_indicator_file.readline()
                    fif_data = fif.split(",")

                    if len(fif) <= 0:
                        self.use_sql.insert_fina_indicator_data("%s_fif" % ts_code,
                                                                fina_indicator_data_list, fif_count - 1)
                        open_fif_fina_indicator_file.close()
                        break

                    elif fif_data[0] == "":
                        continue

                    else:
                        # reordering index
                        fif_data[0] = fif_count - fif_index_count
                        fif_index_count += 1
                        fif_tuple = tuple(fif_data)
                        fina_indicator_data_list.append(fif_tuple)

            # calculate the number of requests API.
            # if the request API reaches 200 times, it will sleep for 60s.
            if get_count == 200:
                time.sleep(60)
                get_count = 0

            else:
                get_count += 1
