#!/usr/bin/env python
# coding=utf-8

"""
@module  : SQLConnect.py
@author  : Rinne
@contact : yejunbin123@qq.com
@time    : 2019 / 01 / 22
"""

import pymysql
import logging


class SQLConnect:
    """Connect mysql and SQL operation.

    this class main to connect mysql to insert stock data and create table.

    Attributes:
    """

    def __init__(self):
        self.connect = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            passwd="123456",
            db="stock",
            charset="utf8",
            autocommit=True
        )

        self.logger = logging.getLogger()
        self.table_tuple = ()
        self.show_table()

    def show_table(self):
        """show stock database tables name.

        this method is make table_tuple save table name.

        :return:
        """
        cursor = self.connect.cursor()
        cursor.execute("show tables")
        self.table_tuple = (tuple(table[0] for table in cursor.fetchall()))

    def insert_stock_data(self, table_name, data, count):
        """insert stock stock price data.

        this method will make stock price data commit to mysql.

        Args：
             :param table_name: insert appoint stock table.
             :param data: insert stock price data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (trade_date, open_price, high_price, low_price, close_price, " \
              "pre_close, stock_change, pct_chg, vol, amount) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
            .format(table_name)

        create_SQL = "create table {0} (trade_date varchar (15) primary key," \
                     "open_price varchar (15), high_price varchar (15), low_price varchar (15), " \
                     "close_price varchar (15), pre_close varchar (15), stock_change varchar (15)," \
                     "pct_chg varchar (15), vol varchar (15),amount varchar (15)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock price data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create stock data table successful")
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("create stock data table Error")
            except pymysql.err.IntegrityError:
                self.connect.rollback()
                self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            try:
                cursor.executemany(sql, data)
                self.logger.info("insert stock data successful")
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("insert stock data Error")
            except pymysql.err.IntegrityError:
                self.connect.rollback()
                self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update data.
            cursor.execute("SELECT count(*) FROM %s" % table_name)

            for (x,) in cursor.fetchall():
                table_count = int(x)

            if table_count != count and table_count != 0:
                check_data = []
                check_database_data = []
                missing_index_list = []
                insert_missing_data = []
                missing_index = 0

                # get file data primary key
                for x in data:
                    check_data.append(int(x[0]))

                cursor.execute("SELECT data_index FROM %s" % table_name)

                # get databases data primary key
                for (x,) in cursor.fetchall():
                    check_database_data.append(int(x))

                # check for missing data in the database and sign index
                for x in check_data:
                    if x not in check_database_data:
                        missing_index_list.append(missing_index)
                        missing_index += 1
                        continue
                    else:
                        missing_index += 1
                        continue

                # save missing data
                for x in missing_index_list:
                    insert_missing_data.append(data[int(x)])

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("insert stock financial statements data successful")
                except pymysql.err.ProgrammingError:
                    self.connect.rollback()
                    self.logger.error("insert stock financial statements data Error")
                except pymysql.err.IntegrityError:
                    self.connect.rollback()
                    self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            else:
                return

    def insert_stock_information(self, information, count):
        """insert stock basic information.

        this method will make stock basic information commit to mysql.
        stock_information regular table name.

        Args:
             :param information: get stock basic information data.
             :param count: get stock basic information file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        cursor = self.connect.cursor()
        self.show_table()

        sql = "insert into stock_information (ts_code, symbol, stock_name, area, industry, list_date) " \
              "values (%s, %s, %s, %s, %s, %s)"

        create_SQL = "create table stock_information (ts_code varchar (25) primary key," \
                     "symbol varchar (15), stock_name  varchar (15), area varchar (15), industry varchar (15)," \
                     "list_date varchar (15)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

        if "stock_information" not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock basic information data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create table stock_information successful")
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("create table stock_information Error")
            except pymysql.err.IntegrityError:
                self.connect.rollback()
                self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            try:
                cursor.executemany(sql, information)
                self.logger.info("insert stock information value (%s) successful" % information)
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("insert stock information Error")
            except pymysql.err.IntegrityError:
                self.connect.rollback()
                self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update data.
            cursor.execute("SELECT count(*) FROM stock_information")

            for (x,) in cursor.fetchall():
                table_count = int(x)

            if table_count != count and table_count != 0:
                check_data = []
                check_database_data = []
                missing_index_list = []
                insert_missing_data = []
                missing_index = 0

                # get file data primary key
                for x in information:
                    check_data.append(int(x[0]))

                cursor.execute("SELECT data_index FROM stock_information")

                # get databases data primary key
                for (x,) in cursor.fetchall():
                    check_database_data.append(int(x))

                # check for missing data in the database and sign index
                for x in check_data:
                    if x not in check_database_data:
                        missing_index_list.append(missing_index)
                        missing_index += 1
                        continue
                    else:
                        missing_index += 1
                        continue

                # save missing data
                for x in missing_index_list:
                    insert_missing_data.append(information[int(x)])

                try:
                    cursor.executemany(sql, insert_missing_data)
                    self.logger.info("insert stock data successful")
                except pymysql.err.ProgrammingError:
                    self.connect.rollback()
                    self.logger.error("insert stock information Error")
                except pymysql.err.IntegrityError:
                    self.connect.rollback()
                    self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            elif table_count == 0:

                try:
                    cursor.executemany(sql, information)
                    self.logger.info("insert stock financial statements data successful")
                except pymysql.err.ProgrammingError:
                    self.connect.rollback()
                    self.logger.error("insert stock financial statements data Error")
                except pymysql.err.IntegrityError:
                    self.connect.rollback()
                    self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            else:
                return

    def insert_stock_financial_statements(self, financial_statements_table, data, count):
        """insert stock financial statements information.

        this method will make stock financial statements information commit to mysql.

        Args:
            :param financial_statements_table: insert appoint stock financial statements table.
            :param data: get stock financial statements data
            :param count: get stock financial statements data file line

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        cursor = self.connect.cursor()
        self.show_table()
        table_count = 0

        sql = "insert into {0} " \
              "(data_index, ts_code, ann_date, f_ann_date, end_date, report_type, comp_type," \
              "basic_eps, diluted_eps, total_revenue, revenue, int_income, prem_earned, comm_income, n_commis_income," \
              "n_oth_income, n_oth_b_income, prem_income, out_prem, une_prem_reser, reins_income, n_sec_tb_income," \
              "n_sec_uw_income, n_asset_mg_income, oth_b_income, fv_value_chg_gain, invest_income, ass_invest_income," \
              "forex_gain, total_cogs, oper_cost, int_exp, comm_exp, biz_tax_surchg, sell_exp, admin_exp, fin_exp," \
              "assets_impair_loss, prem_refund, compens_payout, reser_insur_liab, div_payt, reins_exp, oper_exp," \
              "compens_payout_refu, insur_reser_refu, reins_cost_refund, other_bus_cost, operate_profit," \
              "non_oper_income, non_oper_exp, nca_disploss, total_profit, income_tax, n_income, n_income_attr_p," \
              "minority_gain, oth_compr_income, t_compr_income, compr_inc_attr_p, compr_inc_attr_m_s, ebit, ebitda," \
              "insurance_exp, undist_profit, distable_profit)" \
              "values" \
              "(%s, %s, %s, %s ,%s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s)".format(financial_statements_table)

        create_SQL = "create table {0} " \
                     "(data_index int primary key, ts_code varchar (15), ann_date varchar (15), " \
                     "f_ann_date varchar (15), end_date varchar (15), report_type varchar (15)," \
                     "comp_type varchar (15), basic_eps varchar (15), diluted_eps varchar (15)," \
                     "total_revenue varchar (15), revenue varchar (15), int_income varchar (15)," \
                     "prem_earned varchar (15), comm_income varchar (15), n_commis_income varchar (15)," \
                     "n_oth_income varchar (15), n_oth_b_income varchar (15), prem_income varchar (15)," \
                     "out_prem varchar (15), une_prem_reser varchar (15), reins_income varchar (15)," \
                     "n_sec_tb_income varchar (15), n_sec_uw_income varchar (15), n_asset_mg_income varchar (15)," \
                     "oth_b_income varchar (15), fv_value_chg_gain varchar (15), invest_income varchar (15)," \
                     "ass_invest_income varchar (15), forex_gain varchar (15), total_cogs varchar (15)," \
                     "oper_cost varchar (15), int_exp varchar (15), comm_exp varchar (15)," \
                     "biz_tax_surchg varchar (15), sell_exp varchar (15), admin_exp varchar (15)," \
                     "fin_exp varchar (15), assets_impair_loss varchar (15), prem_refund varchar (15)," \
                     "compens_payout varchar (15), reser_insur_liab varchar (15), div_payt varchar (15)," \
                     "reins_exp varchar (15), oper_exp varchar (15), compens_payout_refu varchar (15)," \
                     "insur_reser_refu varchar (15), reins_cost_refund varchar (15), other_bus_cost varchar (15)," \
                     "operate_profit varchar (15), non_oper_income varchar (15), non_oper_exp varchar (15)," \
                     "nca_disploss varchar (15), total_profit varchar (15), income_tax varchar (15)," \
                     "n_income varchar (15), n_income_attr_p varchar (15), minority_gain varchar (15)," \
                     "oth_compr_income varchar (15), t_compr_income varchar (15), compr_inc_attr_p varchar (15)," \
                     "compr_inc_attr_m_s varchar (15), ebit varchar (15), ebitda varchar (15)," \
                     "insurance_exp varchar (15), undist_profit varchar (15), distable_profit varchar (15))" \
                     .format(financial_statements_table)

        if str.lower(financial_statements_table) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create stock financial statements table successful")
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("create table financial_statements_table) Error")

            try:
                cursor.executemany(sql, data)
                self.logger.info("insert stock financial statements data successful")
            except pymysql.err.ProgrammingError:
                self.connect.rollback()
                self.logger.error("insert stock financial statements data Error")
            except pymysql.err.IntegrityError:
                self.connect.rollback()
                self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
            cursor.execute("SELECT count(*) FROM %s" % financial_statements_table)

            for (x,) in cursor.fetchall():
                table_count = int(x)

            if table_count != count and table_count != 0:
                check_data = []
                check_database_data = []
                missing_index_list = []
                insert_missing_data = []
                missing_index = 0

                # get file data primary key
                for x in data:
                    check_data.append(int(x[0]))

                cursor.execute("SELECT data_index FROM %s" % financial_statements_table)

                # get databases data primary key
                for (x,) in cursor.fetchall():
                    check_database_data.append(int(x))

                # check for missing data in the database and sign index
                for x in check_data:
                    if x not in check_database_data:
                        missing_index_list.append(missing_index)
                        missing_index += 1
                        continue
                    else:
                        missing_index += 1
                        continue

                # save missing data
                for x in missing_index_list:
                    insert_missing_data.append(data[int(x)])

                try:
                    cursor.executemany(sql, insert_missing_data)
                    self.logger.info("insert stock financial statements data successful")
                except pymysql.err.ProgrammingError:
                    self.connect.rollback()
                    self.logger.error("insert stock financial statements data Error")
                except pymysql.err.IntegrityError:
                    self.connect.rollback()
                    self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("insert stock financial statements data successful")
                except pymysql.err.ProgrammingError:
                    self.connect.rollback()
                    self.logger.error("insert stock financial statements data Error")
                except pymysql.err.IntegrityError:
                    self.connect.rollback()
                    self.logger.error("pymysql.err.IntegrityError:insert stock financial statements data Error")

            else:
                return
