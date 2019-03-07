#!/usr/bin/env python
# coding=utf-8

"""
@module  : SQLConnect.py
@author  : Rinne
@contact : yejunbin123@qq.com
@time    : 2019 / 01 / 22
"""

import logging

import pymysql


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

        create_SQL = "create table {0} (trade_date varchar (20) primary key," \
                     "open_price varchar (20), high_price varchar (20), low_price varchar (20), " \
                     "close_price varchar (20), pre_close varchar (20), stock_change varchar (20)," \
                     "pct_chg varchar (20), vol varchar (20),amount varchar (20)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock price data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

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

                cursor.execute("SELECT trade_date FROM %s" % table_name)

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
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)


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
                     "symbol varchar (20), stock_name  varchar (20), area varchar (20), industry varchar (20)," \
                     "list_date varchar (20)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

        if "stock_information" not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock basic information data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, information)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

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
                    cursor.executemany(sql, information)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:

                try:
                    cursor.executemany(sql, information)
                except Exception as ex:
                    self.logger.error(ex)


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
                     "(data_index int primary key, ts_code varchar (20), ann_date varchar (20), " \
                     "f_ann_date varchar (20), end_date varchar (20), report_type varchar (20)," \
                     "comp_type varchar (20), basic_eps varchar (20), diluted_eps varchar (20)," \
                     "total_revenue varchar (20), revenue varchar (20), int_income varchar (20)," \
                     "prem_earned varchar (20), comm_income varchar (20), n_commis_income varchar (20)," \
                     "n_oth_income varchar (20), n_oth_b_income varchar (20), prem_income varchar (20)," \
                     "out_prem varchar (20), une_prem_reser varchar (20), reins_income varchar (20)," \
                     "n_sec_tb_income varchar (20), n_sec_uw_income varchar (20), n_asset_mg_income varchar (20)," \
                     "oth_b_income varchar (20), fv_value_chg_gain varchar (20), invest_income varchar (20)," \
                     "ass_invest_income varchar (20), forex_gain varchar (20), total_cogs varchar (20)," \
                     "oper_cost varchar (20), int_exp varchar (20), comm_exp varchar (20)," \
                     "biz_tax_surchg varchar (20), sell_exp varchar (20), admin_exp varchar (20)," \
                     "fin_exp varchar (20), assets_impair_loss varchar (20), prem_refund varchar (20)," \
                     "compens_payout varchar (20), reser_insur_liab varchar (20), div_payt varchar (20)," \
                     "reins_exp varchar (20), oper_exp varchar (20), compens_payout_refu varchar (20)," \
                     "insur_reser_refu varchar (20), reins_cost_refund varchar (20), other_bus_cost varchar (20)," \
                     "operate_profit varchar (20), non_oper_income varchar (20), non_oper_exp varchar (20)," \
                     "nca_disploss varchar (20), total_profit varchar (20), income_tax varchar (20)," \
                     "n_income varchar (20), n_income_attr_p varchar (20), minority_gain varchar (20)," \
                     "oth_compr_income varchar (20), t_compr_income varchar (20), compr_inc_attr_p varchar (20)," \
                     "compr_inc_attr_m_s varchar (20), ebit varchar (20), ebitda varchar (20)," \
                     "insurance_exp varchar (20), undist_profit varchar (20), distable_profit varchar (20))" \
            .format(financial_statements_table)

        if str.lower(financial_statements_table) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create stock financial statements table successful")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

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
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            else:
                return

    def insert_debt_data(self, table_name, data, count):
        """insert assets debt data.

        this method will make assets debt data commit to mysql.

        Args：
             :param table_name: insert assets debt table.
             :param data: insert assets debt data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (data_index, ts_code, ann_date, f_ann_date, end_date, report_type, comp_type, " \
              "total_share, cap_rese, undistr_porfit, surplus_rese, special_rese, money_cap, trad_asset," \
              "notes_receiv, accounts_receiv, oth_receiv, prepayment, div_receiv, int_receiv, inventories, " \
              "amor_exp, nca_within_1y, sett_rsrv, loanto_oth_bank_fi, premium_receiv, reinsur_receiv, " \
              "reinsur_res_receiv, pur_resale_fa, oth_cur_assets, total_cur_assets, fa_avail_for_sale, htm_invest," \
              "lt_eqt_invest, invest_real_estate, time_deposits, oth_assets, lt_rec, fix_assets, cip ,const_materials," \
              "fixed_assets_disp, produc_bio_assets, oil_and_gas_assets, intan_assets, r_and_d, goodwill ," \
              "lt_amor_exp, defer_tax_assets, decr_in_disbur, oth_nca, total_nca, cash_reser_cb, depos_in_oth_bfi, " \
              "prec_metals, deriv_assets, rr_reins_une_prem, rr_reins_outstd_cla, rr_reins_lins_liab," \
              "rr_reins_lthins_liab, refund_depos, ph_pledge_loans, refund_cap_depos, indep_acct_assets, client_depos," \
              "client_prov, transac_seat_fee, invest_as_receiv, total_assets, lt_borr, st_borr, cb_borr, " \
              "depos_ib_deposits, loan_oth_bank, trading_fl, notes_payable, acct_payable, adv_receipts, " \
              "sold_for_repur_fa, comm_payable, payroll_payable, taxes_payable, int_payable, div_payable, " \
              "oth_payable, acc_exp, deferred_inc, st_bonds_payable, payable_to_reinsurer, rsrv_insur_cont, " \
              "acting_trading_sec, acting_uw_sec, non_cur_liab_due_1y, oth_cur_liab, total_cur_liab, bond_payable, " \
              "lt_payable, specific_payables, estimated_liab, defer_tax_liab, defer_inc_non_cur_liab, oth_ncl, " \
              "total_ncl, depos_oth_bfi, deriv_liab, depos, agency_bus_liab, oth_liab, prem_receiv_adva, " \
              "depos_received, ph_invest, reser_une_prem, reser_outstd_claims, reser_lins_liab, reser_lthins_liab," \
              "indept_acc_liab, pledge_borr, indem_payable, policy_div_payable, total_liab, treasury_share, " \
              "ordin_risk_reser, forex_differ, invest_loss_unconf, minority_int, total_hldr_eqy_exc_min_int, " \
              "total_hldr_eqy_inc_min_int, total_liab_hldr_eqy, lt_payroll_payable, oth_comp_income, oth_eqt_tools ," \
              "oth_eqt_tools_p_shr, lending_funds, acc_receivable, st_fin_payable, payables, hfs_assets, hfs_sales) " \
              "values " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s)" \
            .format(table_name)

        create_SQL = "create table {0} (data_index int primary key,ts_code varchar (20)," \
                     "ann_date varchar (20), f_ann_date varchar (20), end_date varchar (20), " \
                     "report_type varchar (20), comp_type varchar (20), total_share varchar (20)," \
                     "cap_rese varchar (20), undistr_porfit varchar (20), surplus_rese varchar (20)," \
                     "special_rese varchar (20), money_cap varchar (20), trad_asset varchar (20)," \
                     "notes_receiv varchar (20), accounts_receiv varchar (20), oth_receiv varchar (20)," \
                     "prepayment varchar (20), div_receiv varchar (20), int_receiv varchar (20)," \
                     "inventories varchar (20), amor_exp varchar (20), nca_within_1y varchar (20)," \
                     "sett_rsrv varchar (20), loanto_oth_bank_fi varchar (20),premium_receiv varchar (20)," \
                     "reinsur_receiv varchar (20), reinsur_res_receiv varchar (20),pur_resale_fa varchar (20)," \
                     "oth_cur_assets varchar (20), total_cur_assets varchar (20),fa_avail_for_sale varchar (20)," \
                     "htm_invest varchar (20), lt_eqt_invest varchar (20),invest_real_estate varchar (20)," \
                     "time_deposits varchar (20), oth_assets varchar (20),lt_rec varchar (20)," \
                     "fix_assets varchar (20), cip varchar (20),const_materials varchar (20)," \
                     "fixed_assets_disp varchar (20), produc_bio_assets varchar (20),oil_and_gas_assets varchar (20)," \
                     "intan_assets varchar (20), r_and_d varchar (20),goodwill varchar (20)," \
                     "lt_amor_exp varchar (20), defer_tax_assets varchar (20),decr_in_disbur varchar (20)," \
                     "oth_nca varchar (20), total_nca varchar (20),cash_reser_cb varchar (20)," \
                     "depos_in_oth_bfi varchar (20), prec_metals varchar (20),deriv_assets varchar (20)," \
                     "rr_reins_une_prem varchar (20), rr_reins_outstd_cla varchar (20),rr_reins_lins_liab varchar (20)," \
                     "rr_reins_lthins_liab varchar (20), refund_depos varchar (20),ph_pledge_loans varchar (20)," \
                     "refund_cap_depos varchar (20), indep_acct_assets varchar (20),client_depos varchar (20)," \
                     "client_prov varchar (20), transac_seat_fee varchar (20),invest_as_receiv varchar (20)," \
                     "total_assets varchar (20), lt_borr varchar (20),st_borr varchar (20)," \
                     "cb_borr varchar (20), depos_ib_deposits varchar (20),loan_oth_bank varchar (20)," \
                     "trading_fl varchar (20), notes_payable varchar (20),acct_payable varchar (20)," \
                     "adv_receipts varchar (20), sold_for_repur_fa varchar (20),comm_payable varchar (20)," \
                     "payroll_payable varchar (20), taxes_payable varchar (20),int_payable varchar (20)," \
                     "div_payable varchar (20), oth_payable varchar (20),acc_exp varchar (20)," \
                     "deferred_inc varchar (20), st_bonds_payable varchar (20),payable_to_reinsurer varchar (20)," \
                     "rsrv_insur_cont varchar (20), acting_trading_sec varchar (20),acting_uw_sec varchar (20)," \
                     "non_cur_liab_due_1y varchar (20), oth_cur_liab varchar (20),total_cur_liab varchar (20)," \
                     "bond_payable varchar (20), lt_payable varchar (20),specific_payables varchar (20)," \
                     "estimated_liab varchar (20), defer_tax_liab varchar (20),defer_inc_non_cur_liab varchar (20)," \
                     "oth_ncl varchar (20), total_ncl varchar (20),depos_oth_bfi varchar (20)," \
                     "deriv_liab varchar (20), depos varchar (20),agency_bus_liab varchar (20)," \
                     "oth_liab varchar (20), prem_receiv_adva varchar (20),depos_received varchar (20)," \
                     "ph_invest varchar (20), reser_une_prem varchar (20),reser_outstd_claims varchar (20)," \
                     "reser_lins_liab varchar (20), reser_lthins_liab varchar (20),indept_acc_liab varchar (20)," \
                     "pledge_borr varchar (20), indem_payable varchar (20),policy_div_payable varchar (20)," \
                     "total_liab varchar (20), treasury_share varchar (20),ordin_risk_reser varchar (20)," \
                     "forex_differ varchar (20), invest_loss_unconf varchar (20),minority_int varchar (20)," \
                     "total_hldr_eqy_exc_min_int varchar (20), total_hldr_eqy_inc_min_int varchar (20)," \
                     "total_liab_hldr_eqy varchar (20), lt_payroll_payable varchar (20), oth_comp_income varchar (20)," \
                     "oth_eqt_tools varchar (20), oth_eqt_tools_p_shr varchar (20), lending_funds varchar (20)," \
                     "acc_receivable varchar (20), st_fin_payable varchar (20), payables varchar (20)," \
                     "hfs_assets varchar (20), hfs_sales varchar (20)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            else:
                return

    def insert_cashflow_data(self, table_name, data, count):
        """insert cashflow data.

        this method will make cashflow data commit to mysql.

        Args:
            :param table_name: insert cashflow table.
            :param data: get cashflow data
            :param count: get cashflow data file line

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        cursor = self.connect.cursor()
        self.show_table()
        table_count = 0

        sql = "insert into {0} " \
              "(data_index , ts_code , ann_date , " \
              "f_ann_date , end_date , comp_type , " \
              "report_type ,net_profit , finan_exp , " \
              "c_fr_sale_sg , recp_tax_rends , n_depos_incr_fi , " \
              "n_incr_loans_cb , n_inc_borr_oth_fi , prem_fr_orig_contr , " \
              "n_incr_insured_dep ,n_reinsur_prem , n_incr_disp_tfa , " \
              "ifc_cash_incr , n_incr_disp_faas , n_incr_loans_oth_bank , " \
              "n_cap_incr_repur , c_fr_oth_operate_a ,c_inf_fr_operate_a , " \
              "c_paid_goods_s , c_paid_to_for_empl , c_paid_for_taxes , " \
              "n_incr_clt_loan_adv , n_incr_dep_cbob , " \
              "c_pay_claims_orig_inco , pay_handling_chrg , " \
              "pay_comm_insur_plcy , oth_cash_pay_oper_act , " \
              "st_cash_out_act , n_cashflow_act , oth_recp_ral_inv_act ," \
              "c_disp_withdrwl_invest , c_recp_return_invest , " \
              "n_recp_disp_fiolta , n_recp_disp_sobu , " \
              "stot_inflows_inv_act , c_pay_acq_const_fiolta , " \
              "c_paid_invest , n_disp_subs_oth_biz , oth_pay_ral_inv_act , " \
              "n_incr_pledge_loan , stot_out_inv_act , " \
              "n_cashflow_inv_act , c_recp_borrow , proc_issue_bonds ," \
              "oth_cash_recp_ral_fnc_act , stot_cash_in_fnc_act , " \
              "free_cashflow , c_prepay_amt_borr , " \
              "c_pay_dist_dpcp_int_exp , incl_dvd_profit_paid_sc_ms , " \
              "oth_cashpay_ral_fnc_act , stot_cashout_fnc_act , " \
              "n_cash_flows_fnc_act , eff_fx_flu_cash , " \
              "n_incr_cash_cash_equ , c_cash_equ_beg_period , " \
              "c_cash_equ_end_period , c_recp_cap_contrib ," \
              "incl_cash_rec_saims , uncon_invest_loss , " \
              "prov_depr_assets , depr_fa_coga_dpba , " \
              "amort_intang_assets , lt_amort_deferred_exp , " \
              "decr_deferred_exp , incr_acc_exp , loss_disp_fiolta , " \
              "loss_scr_fa , loss_fv_chg , invest_loss , " \
              "decr_def_inc_tax_assets ,incr_def_inc_tax_liab ," \
              "decr_inventories , decr_oper_payable , incr_oper_payable , " \
              "others , im_net_cashflow_oper_act , conv_debt_into_cap , " \
              "conv_copbonds_due_within_1y , fa_fnc_leases , " \
              "end_bal_cash , beg_bal_cash , end_bal_cash_equ , " \
              "beg_bal_cash_equ , im_n_incr_cash_equ)" \
              "values" \
              "(%s, %s, %s, %s, %s ,%s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s," \
              "%s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s)".format(table_name)

        create_SQL = "create table {0} " \
                     "(data_index int primary key, ts_code varchar (20), ann_date varchar (20), " \
                     "f_ann_date varchar (20), end_date varchar (20), comp_type varchar (20), " \
                     "report_type varchar (20),net_profit varchar (20), finan_exp varchar (20), " \
                     "c_fr_sale_sg varchar (20), recp_tax_rends varchar (20), n_depos_incr_fi varchar (20), " \
                     "n_incr_loans_cb varchar (20), n_inc_borr_oth_fi varchar (20), prem_fr_orig_contr varchar (20), " \
                     "n_incr_insured_dep varchar (20),n_reinsur_prem varchar (20), n_incr_disp_tfa varchar (20), " \
                     "ifc_cash_incr varchar (20), n_incr_disp_faas varchar (20), n_incr_loans_oth_bank varchar (20), " \
                     "n_cap_incr_repur varchar (20), c_fr_oth_operate_a varchar (20),c_inf_fr_operate_a varchar (20), " \
                     "c_paid_goods_s varchar (20), c_paid_to_for_empl varchar (20), c_paid_for_taxes varchar (20), " \
                     "n_incr_clt_loan_adv varchar (20), n_incr_dep_cbob varchar (20), " \
                     "c_pay_claims_orig_inco varchar (20), pay_handling_chrg varchar (20), " \
                     "pay_comm_insur_plcy varchar (20), oth_cash_pay_oper_act varchar (20), " \
                     "st_cash_out_act varchar (20), n_cashflow_act varchar (20), oth_recp_ral_inv_act varchar (20)," \
                     "c_disp_withdrwl_invest varchar (20), c_recp_return_invest varchar (20), " \
                     "n_recp_disp_fiolta varchar (20), n_recp_disp_sobu varchar (20), " \
                     "stot_inflows_inv_act varchar (20), c_pay_acq_const_fiolta varchar (20), " \
                     "c_paid_invest varchar (20), n_disp_subs_oth_biz varchar (20), oth_pay_ral_inv_act varchar (20), " \
                     "n_incr_pledge_loan varchar (20), stot_out_inv_act varchar (20), " \
                     "n_cashflow_inv_act varchar (20), c_recp_borrow varchar (20), proc_issue_bonds varchar (20)," \
                     "oth_cash_recp_ral_fnc_act varchar (20), stot_cash_in_fnc_act varchar (20), " \
                     "free_cashflow varchar (20), c_prepay_amt_borr varchar (20), " \
                     "c_pay_dist_dpcp_int_exp varchar (20), incl_dvd_profit_paid_sc_ms varchar (20), " \
                     "oth_cashpay_ral_fnc_act varchar (20), stot_cashout_fnc_act varchar (20), " \
                     "n_cash_flows_fnc_act varchar (20), eff_fx_flu_cash varchar (20), " \
                     "n_incr_cash_cash_equ varchar (20), c_cash_equ_beg_period varchar (20), " \
                     "c_cash_equ_end_period varchar (20), c_recp_cap_contrib varchar (20)," \
                     "incl_cash_rec_saims varchar (20), uncon_invest_loss varchar (20), " \
                     "prov_depr_assets varchar (20), depr_fa_coga_dpba varchar (20), " \
                     "amort_intang_assets varchar (20), lt_amort_deferred_exp varchar (20), " \
                     "decr_deferred_exp varchar (20), incr_acc_exp varchar (20), loss_disp_fiolta varchar (20), " \
                     "loss_scr_fa varchar (20), loss_fv_chg varchar (20), invest_loss varchar (20), " \
                     "decr_def_inc_tax_assets varchar (20),incr_def_inc_tax_liab varchar (20)," \
                     "decr_inventories varchar (20), decr_oper_payable varchar (20), incr_oper_payable varchar (20), " \
                     "others varchar (20), im_net_cashflow_oper_act varchar (20), conv_debt_into_cap varchar (20), " \
                     "conv_copbonds_due_within_1y varchar (20), fa_fnc_leases varchar (20), " \
                     "end_bal_cash varchar (20), beg_bal_cash varchar (20), end_bal_cash_equ varchar (20), " \
                     "beg_bal_cash_equ varchar (20), im_n_incr_cash_equ varchar (20))" \
            .format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create cashflow table successful")
            except Exception as ex:
                self.logger.error(ex)
            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)
            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)


            else:
                return

    def insert_forecast_data(self, table_name, data, count):
        """insert forecast data.

        this method will make forecast data commit to mysql.

        Args：
             :param table_name: insert forecast table.
             :param data: insert forecast data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        # filename = "testsql.txt"
        # with open(filename, "w") as f:
        #     for x in data:
        #         a = str(x)
        #         string = "INSERT INTO 20011231_fc VALUES " + a + ";"
        #         print(string)
        #         f.write(string)
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (data_index, ts_code, ann_date, end_date, type, p_change_min, p_change_max, " \
              "net_profit_min, net_profit_max, last_parent_net, first_ann_date, summary, change_reason) " \
              "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

        create_SQL = "create table {0} (data_index int primary key, ts_code varchar (30)," \
                     "ann_date varchar (30), end_date varchar (30), type varchar (30), " \
                     "p_change_min varchar (30), p_change_max varchar (30), net_profit_min varchar (30)," \
                     "net_profit_max varchar (30), last_parent_net varchar (30), first_ann_date varchar (30)," \
                     "summary varchar (3000), change_reason varchar (3000)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)


            else:
                return

    def insert_dividend_data(self, table_name, data, count):
        """insert dividend data.

        this method will make dividend data commit to mysql.

        Args：
             :param table_name: insert dividend table.
             :param data: insert dividend data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (data_index, ts_code , end_date, ann_date, div_proc, stk_div, stk_bo_rate, stk_co_rate," \
              "cash_div, cash_div_tax, record_date, ex_date, pay_date, div_listdate, imp_ann_date) " \
              "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

        create_SQL = "create table {0} (data_index int primary key,ts_code varchar (20)," \
                     "end_date varchar (20), ann_date varchar (20), div_proc varchar (20), " \
                     "stk_div varchar (20), stk_bo_rate varchar (20), stk_co_rate varchar (20)," \
                     "cash_div varchar (20), cash_div_tax varchar (20), record_date varchar (20)," \
                     "ex_date varchar (20), pay_date varchar (20), div_listdate varchar (20)," \
                     "imp_ann_date varchar (20), base_date varchar (20), base_share varchar (20)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            else:
                return

    def insert_express_data(self, table_name, data, count):
        """insert express data.

        this method will make express data commit to mysql.

        Args：
             :param table_name: insert express table.
             :param data: insert express data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (data_index, ts_code, ann_date, end_date, revenue, operate_profit, total_profit, " \
              "n_income, total_assets, total_hldr_eqy_exc_min_int, diluted_eps, diluted_roe, yoy_net_profit, bps," \
              "yoy_sales, yoy_op, yoy_tp, yoy_dedu_np, yoy_eps, yoy_roe, growth_assets, yoy_equity, growth_bps," \
              "or_last_year, op_last_year, tp_last_year, np_last_year, eps_last_year, open_net_assets, open_bps," \
              "perf_summary , is_audit, remark) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

        create_SQL = "create table {0} (data_index int primary key,ts_code varchar (20)," \
                     "ann_date varchar (20), end_date varchar (20), revenue varchar (20), " \
                     "operate_profit varchar (20), total_profit varchar (20), n_income varchar (20)," \
                     "total_assets varchar (20), total_hldr_eqy_exc_min_int varchar (20), diluted_eps varchar (20)," \
                     "diluted_roe varchar (20), yoy_net_profit varchar (20), bps varchar (20)," \
                     "yoy_sales varchar (20), yoy_op varchar (20), yoy_tp varchar (20)," \
                     "yoy_dedu_np varchar (20), yoy_eps varchar (20), yoy_roe varchar (20)," \
                     "growth_assets varchar (20), yoy_equity varchar (20), growth_bps varchar (20)," \
                     "or_last_year varchar (20), op_last_year varchar (20),tp_last_year varchar (20)," \
                     "np_last_year varchar (20), eps_last_year varchar (20),open_net_assets varchar (20)," \
                     "open_bps varchar (20),perf_summary  varchar (20),is_audit varchar (20)," \
                     "remark varchar (20)) ENGINE=InnoDB " \
                     "DEFAULT CHARSET=utf8;".format(table_name)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("create express table successful")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)


        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            else:
                return

    def insert_fina_indicator_data(self, table_name, data, count):
        """insert fina_indicator data.

        this method will make fina_indicator data commit to mysql.

        Args：
             :param table_name: insert fina_indicator table.
             :param data: insert fina_indicator data.
             :param count: get file line to check up, which check to see if updates are needed.

        Raises:
               pymysql.err.ProgrammingError: if anything happens in programming at commit, databases will rollback.

        :return:
        """
        table_count = 0
        self.show_table()

        cursor = self.connect.cursor()

        sql = "insert into {0} (data_index,ts_code,ann_date," \
              "end_date,eps,dt_eps," \
              "total_revenue_ps,revenue_ps,capital_rese_ps," \
              "surplus_rese_ps,undist_profit_ps,extra_item," \
              "profit_dedt,gross_margin,current_ratio," \
              "quick_ratio,cash_ratio,ar_turn," \
              "ca_turn,fa_turn,assets_turn," \
              "op_income,ebit,ebitda," \
              "fcff,fcfe,current_exint," \
              "noncurrent_exint,interestdebt,netdebt," \
              "tangible_asset,working_capital,networking_capital," \
              "invest_capital,retained_earnings,diluted2_eps," \
              "bps,ocfps,retainedps," \
              "cfps,ebit_ps,fcff_ps," \
              "fcfe_ps,netprofit_margin,grossprofit_margin," \
              "cogs_of_sales,expense_of_sales,profit_to_gr," \
              "saleexp_to_gr,adminexp_of_gr,finaexp_of_gr," \
              "impai_ttm,gc_of_gr,op_of_gr," \
              "ebit_of_gr,roe,roe_waa," \
              "roe_dt,roa,npta," \
              "roic,roe_yearly,roa2_yearly," \
              "debt_to_assets,assets_to_eqt,dp_assets_to_eqt," \
              "ca_to_assets,nca_to_assets,tbassets_to_totalassets," \
              "int_to_talcap,eqt_to_talcapital,currentdebt_to_debt," \
              "longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt," \
              "eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt," \
              "tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt," \
              "turn_days,roa_yearly,roa_dp," \
              "fixed_assets,profit_to_op,q_saleexp_to_gr," \
              "q_gc_to_gr,q_roe,q_dt_roe," \
              "q_npta,q_ocf_to_sales,basic_eps_yoy," \
              "dt_eps_yoy,cfps_yoy,op_yoy," \
              "ebt_yoy,netprofit_yoy,dt_netprofit_yoy," \
              "ocf_yoy,roe_yoy,bps_yoy," \
              "assets_yoy,eqt_yoy,tr_yoy," \
              "or_yoy,q_sales_yoy,q_op_qoq," \
              "equity_yoy) " \
              "values" \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s)" \
            .format(table_name)

        create_SQL = "create table {0} (data_index int primary key,ts_code varchar(20),ann_date varchar(20)," \
                     "end_date varchar(20),eps varchar(20),dt_eps varchar(20)," \
                     "total_revenue_ps varchar(20),revenue_ps varchar(20),capital_rese_ps varchar(20)," \
                     "surplus_rese_ps varchar(20),undist_profit_ps varchar(20),extra_item varchar(20)," \
                     "profit_dedt varchar(20),gross_margin varchar(20),current_ratio varchar(20)," \
                     "quick_ratio varchar(20),cash_ratio varchar(20),ar_turn varchar(20)," \
                     "ca_turn varchar(20),fa_turn varchar(20),assets_turn varchar(20)," \
                     "op_income varchar(20),ebit varchar(20),ebitda varchar(20)," \
                     "fcff varchar(20),fcfe varchar(20),current_exint varchar(20)," \
                     "noncurrent_exint varchar(20),interestdebt varchar(20),netdebt varchar(20)," \
                     "tangible_asset varchar(20),working_capital varchar(20),networking_capital varchar(20)," \
                     "invest_capital varchar(20),retained_earnings varchar(20),diluted2_eps varchar(20)," \
                     "bps varchar(20),ocfps varchar(20),retainedps varchar(20)," \
                     "cfps varchar(20),ebit_ps varchar(20),fcff_ps varchar(20)," \
                     "fcfe_ps varchar(20),netprofit_margin varchar(20),grossprofit_margin varchar(20)," \
                     "cogs_of_sales varchar(20),expense_of_sales varchar(20),profit_to_gr varchar(20)," \
                     "saleexp_to_gr varchar(20),adminexp_of_gr varchar(20),finaexp_of_gr varchar(20)," \
                     "impai_ttm varchar(20),gc_of_gr varchar(20),op_of_gr varchar(20)," \
                     "ebit_of_gr varchar(20),roe varchar(20),roe_waa varchar(20)," \
                     "roe_dt varchar(20),roa varchar(20),npta varchar(20)," \
                     "roic varchar(20),roe_yearly varchar(20),roa2_yearly varchar(20)," \
                     "debt_to_assets varchar(20),assets_to_eqt varchar(20),dp_assets_to_eqt varchar(20)," \
                     "ca_to_assets varchar(20),nca_to_assets varchar(20),tbassets_to_totalassets varchar(20)," \
                     "int_to_talcap varchar(20),eqt_to_talcapital varchar(20),currentdebt_to_debt varchar(20)," \
                     "longdeb_to_debt varchar(20),ocf_to_shortdebt varchar(20),debt_to_eqt varchar(20)," \
                     "eqt_to_debt varchar(20),eqt_to_interestdebt varchar(20),tangibleasset_to_debt varchar(20)," \
                     "tangasset_to_intdebt varchar(20),tangibleasset_to_netdebt varchar(20),ocf_to_debt varchar(20)," \
                     "turn_days varchar(20),roa_yearly varchar(20),roa_dp varchar(20)," \
                     "fixed_assets varchar(20),profit_to_op varchar(20),q_saleexp_to_gr varchar(20)," \
                     "q_gc_to_gr varchar(20),q_roe varchar(20),q_dt_roe varchar(20)," \
                     "q_npta varchar(20),q_ocf_to_sales varchar(20),basic_eps_yoy varchar(20)," \
                     "dt_eps_yoy varchar(20),cfps_yoy varchar(20),op_yoy varchar(20)," \
                     "ebt_yoy varchar(20),netprofit_yoy varchar(20),dt_netprofit_yoy varchar(20)," \
                     "ocf_yoy varchar(20),roe_yoy varchar(20),bps_yoy varchar(20)," \
                     "assets_yoy varchar(20),eqt_yoy varchar(20),tr_yoy varchar(20)," \
                     "or_yoy varchar(20),q_sales_yoy varchar(20),q_op_qoq varchar(20)," \
                     "equity_yoy varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=utf8;".format(table_name)

        # filename = "testsql.txt"
        #
        # with open(filename, "w") as f:
        #     for x in data:
        #         a = str(x)
        #         string = "INSERT INTO 000001_SZ_fif VALUES " + a + ";"
        #         print(string)
        #         f.write(string)

        if str.lower(table_name) not in self.table_tuple:
            # check whether the table name exists.
            # if the table exists will check to see if updates are needed.
            # if the table does not exist, program will create table and insert stock financial statements data.
            try:
                cursor.execute(create_SQL)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)

            try:
                cursor.executemany(sql, data)
                self.logger.info("")
            except Exception as ex:
                self.logger.error(ex)


        else:
            # get table line to check to see if updates are needed.
            # if file line same as table line will pass update data.
            # if file line different from table line will update missing data.
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

                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)

            elif table_count == 0:
                try:
                    cursor.executemany(sql, data)
                    self.logger.info("")
                except Exception as ex:
                    self.logger.error(ex)
            else:
                return
