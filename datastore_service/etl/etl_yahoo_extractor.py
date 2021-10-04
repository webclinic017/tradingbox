import time
import asyncio
from requests import RequestException

import pandas as pd
import numpy as np

import yahooquery as yf

from models.base import Symbology
from etl.load_data_symbology import LoadDataSymbologyTask

from api.db_api import database

from settings.config import DATA_TYPE_STOCK
from settings.settings import logger
from settings.settings import LUIGI_CONFIG_PATH

import luigi
luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)

nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']

########################################################################################################################

# Modules which return a dict
"""
module_keys = ['assetProfile', 'summaryProfile', 'summaryDetail', 'price', 'financialData', 'defaultKeyStatistics',
             'quoteType', 'majorHoldersBreakdown', 'netSharePurchaseActivity', 'cashflowStatementHistory',
               'balanceSheetHistory', 'balanceSheetHistoryQuarterly', 'cashflowStatementHistoryQuarterly',
               'incomeStatementHistory', 'incomeStatementHistoryQuarterly', 'earningsHistory', 'secFilings',
               'calendarEvents']
"""
module_keys = ['summaryProfile', 'summaryDetail', 'financialData', 'defaultKeyStatistics',
               'calendarEvents', 'cashflowStatementHistory', 'cashflowStatementHistoryQuarterly', 'balanceSheetHistory',
               'balanceSheetHistoryQuarterly', 'incomeStatementHistory', 'incomeStatementHistoryQuarterly']

dict_keys = ['assetProfile', 'summaryProfile', 'summaryDetail', 'price', 'financialData', 'defaultKeyStatistics',
             'quoteType', 'majorHoldersBreakdown', 'netSharePurchaseActivity']


########################################################################################################################


class ExtractorYahooDataTask(luigi.Task):
    data_type = DATA_TYPE_STOCK
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    symbols_dict = {}
    results = {}

    def requires(self):
        return LoadDataSymbologyTask()

    def output(self, key=None):
        if key is None:
            outputs_list = []
            for key in module_keys:
                outputs_list.append(luigi.LocalTarget(f'{self.data_dir}/{key}_yahoo_data.csv'))
            return outputs_list
        else:
            return luigi.LocalTarget(f'{self.data_dir}/{key}_yahoo_data.csv')

    async def get_yahoo_symbols(self):
        await database.connect()
        yahoo_symbols = await Symbology.get_by_source('yahoo', self.data_type)
        await database.disconnect()
        return yahoo_symbols

    def run(self):

        logger.info('Processing the Yahoo Extractor from Yahoo Finance... \n')
        start_time = time.time()

        for key in module_keys:
            self.results[key] = pd.DataFrame()

        if self.data_type == DATA_TYPE_STOCK:
            try:
                # get yahoo symbols from Symbology table
                yahoo_symbols = asyncio.run(self.get_yahoo_symbols())
                yahoo_symbols = yahoo_symbols.loc[~yahoo_symbols['source_id'].str.contains('_')]

                # select symbol which are active stock
                yahoo_symbols = yahoo_symbols.loc[yahoo_symbols['is_active'] == 1]
                yahoo_symbols = yahoo_symbols.drop_duplicates(subset=['id'])
                # Create dict with symbol and id
                self.symbols_dict = yahoo_symbols.set_index('source_id').to_dict()['symbol_id']

                symbols = yahoo_symbols['source_id'].to_list()
                # logger.info('\nDownloaded Tickers Data from Yahoo in %0.1f seconds' % (time.time() - start_time))
                # logger.info("Download symbol: %s --- %s to %s from Yahoo." % (symbol, str(idx), str(len(symbols))))
                self.get_ticker_info(symbols)

                logger.info("Finalized to extract all symbols | %0.1f seconds" % (time.time() - start_time))

            except Exception as e:
                logger.info("Something wrong Yahoo Extractor function")
                logger.error(str(e))

        start_time = time.time()
        for key, data in self.results.items():
            if not data.empty:
                with self.output(key).temporary_path() as path:
                    data.to_csv(path, sep="\t", header=True)

        logger.info('\nComplete YAHOO EXTRACTOR process in %0.1f seconds' % (time.time() - start_time))

#######################################################################################################################

    def get_ticker_info(self, ticker_list):

        modules = ' '.join(module_keys)

        def get_yahoo_request(symlist):
            ticker_data = None
            request_ok = False
            count = 0
            while not request_ok and count < 5:
                try:
                    ticker = yf.Ticker(symlist, asynchronous=True)
                    ticker_data = ticker.get_modules(modules)
                except RequestException as e:
                    new_list = symlist.copy()
                    for tick in new_list:
                        if tick in str(e):
                            symlist.remove(tick)
                except Exception as e:
                    logger.info("Something wrong while downloading data for Yahoo... Try again")
                    logger.error(str(e))
                    count += 1
                if 'error' not in ticker_data.keys():
                    request_ok = True
            return ticker_data

        i = 0
        num = 100

        errors = []
        while i < len(ticker_list):
            logger.info("Download %s to %s from Yahoo..." % (str(i), str(len(ticker_list))))
            sub_list = ticker_list[i:i + num]
            data = get_yahoo_request(sub_list)
            for symbol, row in data.items():
                if type(row) == dict:
                    self.extract_yahooinfo(symbol, row)
                else:
                    errors.append(symbol)
            i += num

        if errors:
            logger.info("Ticker Errors..." + ", ".join(errors))

    def extract_yahooinfo(self, symbol, data):

        for key in data.keys():
            row = data[key]
            row['symbol'] = symbol
            row['symbol_id'] = self.symbols_dict[symbol]

            if key in dict_keys:
                row['companyOfficers'] = ''
                for k in row.keys():
                    if isinstance(row[k], dict) or isinstance(row[k], list):
                        row[k] = ''
                self.results[key] = self.results[key].append(row, ignore_index=True)

            elif key in ['cashflowStatementHistory', 'cashflowStatementHistoryQuarterly']:
                subkey = key.replace("History", "s").replace("Quarterly", "")
                for item in row[subkey]:
                    item['symbol'] = symbol
                    item['symbol_id'] = self.symbols_dict[symbol]
                    self.results[key] = self.results[key].append(item, ignore_index=True)

            elif key in ['balanceSheetHistory', 'balanceSheetHistoryQuarterly']:
                subkey = key.replace("History", "Statements").replace("Quarterly", "")
                for item in row[subkey]:
                    item['symbol'] = symbol
                    item['symbol_id'] = self.symbols_dict[symbol]
                    self.results[key] = self.results[key].append(item, ignore_index=True)

            elif key in ['incomeStatementHistory', 'incomeStatementHistoryQuarterly']:
                subkey = key.replace("Quarterly", "")
                for item in row[subkey]:
                    item['symbol'] = symbol
                    item['symbol_id'] = self.symbols_dict[symbol]
                    self.results[key] = self.results[key].append(item, ignore_index=True)

            elif key in ['earningsHistory']:
                subkey = "history"
                for item in row[subkey]:
                    item['symbol'] = symbol
                    item['symbol_id'] = self.symbols_dict[symbol]
                    self.results[key] = self.results[key].append(item, ignore_index=True)

            elif key in ['secFilings']:
                subkey = "filings"
                for item in row[subkey]:
                    item['symbol'] = symbol
                    item['symbol_id'] = self.symbols_dict[symbol]
                    self.results[key] = self.results[key].append(item, ignore_index=True)

            elif key in ['calendarEvents']:
                earnings = row['earnings']
                dates = earnings['earningsDate']
                if len(dates) >= 2:
                    earnings_date = {'start_earningsDate': dates[0], 'end_earningsDate': dates[1]}
                elif len(dates) == 1:
                    earnings_date = {'start_earningsDate': dates[0], 'end_earningsDate': None}
                else:
                    earnings_date = {'start_earningsDate': None, 'end_earningsDate': None}

                earnings.pop('earningsDate', None)
                row.pop('earnings', None)

                row.update(earnings)
                row.update(earnings_date)
                self.results[key] = self.results[key].append(row, ignore_index=True)
                #
                """
                earnings = row['earnings']
                row['earnings'] = ''
                self.results[key] = self.results[key].append(row, ignore_index=True)

                dates = earnings['earningsDate']
                earnings['earningsDate'] = ''
                earnings['symbol'] = symbol
                earnings['symbol_id'] = self.symbols_dict[symbol]
                self.results['earnings'] = self.results['earnings'].append(earnings, ignore_index=True)
                if len(dates) >= 2:
                    earningsDate = {'start_earningsDate': dates[0], 'end_earningsDate': dates[1]}
                if len(dates) == 1:
                    earningsDate = {'start_earningsDate': dates[0], 'end_earningsDate': None}
                else:
                    earningsDate = {'start_earningsDate': None, 'end_earningsDate': None}
                earningsDate['symbol'] = symbol
                earningsDate['symbol_id'] = self.symbols_dict[symbol]
                self.results['earningsDate'] = self.results['earningsDate'].append(earningsDate, ignore_index=True)
                """
            elif "Trend" in key:  # indexTrend, industryTrend, earningsTrend, sectorTrend, recommendationTrend
                pass

            elif key in ['fundOwnership', 'insiderHolders', 'upgradeDowngradeHistory', 'institutionOwnership',
                         'esgScores']:
                pass
