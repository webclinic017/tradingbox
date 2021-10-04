import time
import asyncio

import pandas as pd
import numpy as np

import luigi
from luigi import LocalTarget
from settings.settings import LUIGI_CONFIG_PATH

from settings.settings import logger
from api.db_api import database

from models.fin_data import Summary, Financials, Fundamentals, Events
from models.fin_data import IncomeAnnual, IncomeQuarterly
from models.fin_data import BalanceSheetAnnual, BalanceSheetQuarterly
from models.fin_data import CashflowAnnual, CashflowQuarterly

from etl.etl_base import SqlTarget
from etl.etl_yahoo_extractor import ExtractorYahooDataTask
from etl.load_initialize import LoadInitializeTask

luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)

nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE', 'Infinity', np.Infinity, np.Inf, -np.Inf]

YAHOO_LOAD_LIST = [
    'summaryProfile_yahoo_data',
    'summaryDetail_yahoo_data',
    'defaultKeyStatistics_yahoo_data',
    'financialData_yahoo_data',
    'calendarEvents_yahoo_data',
    'cashflowStatementHistory_yahoo_data',
    'cashflowStatementHistoryQuarterly_yahoo_data',
    'balanceSheetHistory_yahoo_data',
    'balanceSheetHistoryQuarterly_yahoo_data',
    'incomeStatementHistory_yahoo_data',
    'incomeStatementHistoryQuarterly_yahoo_data'
]

########################################################################################################################
#                                           MAIN PROCEDURE


def get_input_dict(list_input):
    results = []
    for inpt in list_input:
        if type(inpt) is LocalTarget:
            results.append(inpt)
        elif type(inpt) is list:
            for item in inpt:
                results.append(item)
    return results


class LoadFundamentalsTable(luigi.Task):
 #   priority = 70

    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    task_complete = False
    input_df = {}

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def requires(self):
        return [LoadInitializeTask(),
                ExtractorYahooDataTask()]

    def run(self):
        input_value = get_input_dict(self.input())
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                in_file = table_in.path
                if 'yahoo' in in_file:
                    in_name = in_file.replace(self.data_dir + "/", "").replace(".csv", "")
                    if in_name in YAHOO_LOAD_LIST:
                        self.input_df[in_name] = pd.read_csv(in_file, header=0, sep='\t', keep_default_na=False,
                                                             na_values=[''], dtype=str)
                    else:
                        continue
                else:
                    in_name = in_file.replace(self.data_dir + "/", "").replace(".csv", "")
                    self.input_df[in_name] = pd.read_csv(in_file, header=0, sep='\t', keep_default_na=False,
                                                         na_values=[''])
                # Replace infinity values to NaN
                self.input_df[in_name].replace(nothing, np.nan, inplace=True)
                # Replace Nan values to None

                self.input_df[in_name] = self.input_df[in_name].replace({np.nan: None})
            #    self.input_df[in_name] = self.input_df[in_name].where(self.input_df[in_name].notnull(), None)

        logger.info('Loading the FUNDAMENTAL data on database... ')
        start_time = time.time()

        try:
            sumProfile = self.input_df['summaryProfile_yahoo_data'].set_index('symbol_id', drop=True).drop(['symbol'], axis=1)
            sumDetails = self.input_df['summaryDetail_yahoo_data'].set_index('symbol_id', drop=True)
            sumProfile = sumProfile[~sumProfile.index.duplicated(keep='first')]
            sumDetails = sumDetails[~sumDetails.index.duplicated(keep='first')]

            summary = pd.concat([sumProfile, sumDetails], axis=1)
            summary['symbol_id'] = summary.index
            summary.reset_index(drop=True, inplace=True)
            summary.replace(nothing, np.nan, inplace=True)
            # summary = summary.where(summary.notnull(), None)
            summary = summary.replace({np.nan: None})
            logger.info("Loading SUMMARY data...")
            asyncio.run(self.output().load_data(summary, Summary.tablename))
            logger.info("Loading FINANCIALS data...")
            asyncio.run(self.output().load_data(self.input_df['financialData_yahoo_data'],
                                    Financials.tablename))
            logger.info("Loading FUNDAMENTALS data...")
            asyncio.run(self.output().load_data(self.input_df['defaultKeyStatistics_yahoo_data'],
                                    Fundamentals.tablename))
            logger.info("Loading EVENTS data...")
            asyncio.run(self.output().load_data(self.input_df['calendarEvents_yahoo_data'],
                                    Events.tablename))
            logger.info("Loading CASHFLOW ANNUAL data...")
            asyncio.run(self.output().load_data(self.input_df['cashflowStatementHistory_yahoo_data'],
                                    CashflowAnnual.tablename))
            logger.info("Loading CASHFLOW QUATER data...")
            asyncio.run(self.output().load_data(self.input_df['cashflowStatementHistoryQuarterly_yahoo_data'],
                                    CashflowQuarterly.tablename))
            logger.info("Loading BALANCE ANNUAL data...")
            asyncio.run(self.output().load_data(self.input_df['balanceSheetHistory_yahoo_data'],
                                    BalanceSheetAnnual.tablename))
            logger.info("Loading BALANCE QUATER data...")
            asyncio.run(self.output().load_data(self.input_df['balanceSheetHistoryQuarterly_yahoo_data'],
                                    BalanceSheetQuarterly.tablename))
            logger.info("Loading INCOME ANNUAL data...")
            asyncio.run(self.output().load_data(self.input_df['incomeStatementHistory_yahoo_data'],
                                    IncomeAnnual.tablename))
            logger.info("Loading INCOME QUATER data...")
            asyncio.run(self.output().load_data(self.input_df['incomeStatementHistoryQuarterly_yahoo_data'],
                                    IncomeQuarterly.tablename))

        except Exception as e:
            logger.error("Something wrong on Loading Fundamental data on database... Skip it!")
            logger.error(str(e))

        self.task_complete = True
        logger.info('\nComplete Loading FUNDAMENTAL data process in %0.1f seconds' % (time.time() - start_time))

    def output(self):
        return SqlTarget()
