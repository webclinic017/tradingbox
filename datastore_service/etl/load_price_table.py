import time
import json
import asyncio
import aiohttp

from abc import ABC, abstractmethod

from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

import yahooquery as yq

# from api.database_api import SessionLocal_Base, get_database #, get_async_database
from api.db_api import database

from settings.settings import logger, QUANDL_API
from settings.config import DOWNLOAD_LIST, DATA_TYPE_STOCK

# from database.models import TABLES
from models.base import Data_Vendors, Exchanges, Symbology
from models.tickers import Tickers
from models.prices import Daily_Prices, Daily_LastPrices
from models.utils import nothing

from etl.utils.ratelimiter import RateLimiter
from etl.load_ticker_table import LoadTickerTable, LoadIndicesTable

from settings.settings import LUIGI_CONFIG_PATH
from settings.settings import ALPACA_PAPER
import pytz

import luigi

luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)


########################################################################################################################
#                    INIZIALIZE PRICE DOWNLOAD

def build_download_list(args):
    # Build the download list from the argparse arguments provided. There is
    #   probably a much better way to do this
    download_list = []
    if args['daily_downloads'] or args['minute_downloads']:
        daily_d = args['daily_downloads']
        minute_d = args['minute_downloads']
        # Cycle through all download templates specified above
        for source in DOWNLOAD_LIST:
            if daily_d and source['interval'] == 'daily':
                for cur_d in daily_d:
                    # Need to do string to string comparisson
                    if source['source'] in cur_d:
                        # For quandl, ensure selection matches argparse exactly
                        if source['source'] in 'quandl' and \
                                source['selection'] == cur_d[cur_d.find('.') + 1:]:
                            download_list.append(source)
                        elif source['source'] not in 'quandl':
                            download_list.append(source)
            if minute_d and source['source'] in minute_d and \
                    source['interval'] == 'minute':
                download_list.append(source)
    return download_list


today = datetime.today()

# Don't change these variables unless you know what you are doing!
quandl_data_url = ['https://www.quandl.com/api/v1/datasets/', '.csv']

google_fin_url = {'root': 'http://www.google.com/finance/getprices?',
                  'ticker': 'q=',
                  'exchange': 'x=',
                  'interval': 'i=',  # 60; 60 seconds is the shortest interval
                  # 'sessions': 'sessions=ext_hours',
                  'period': 'p=',  # 20d; 15d is the longest period for min
                  'fields': 'f=d,c,v,o,h,l'}  # order doesn't change anything

yahoo_end_date = ('d=%s&e=%s&f=%s' %
                  (str(int(today.month - 1)).zfill(2), today.day, today.year))
yahoo_fin_url = {'root': 'http://real-chart.finance.yahoo.com/table.csv?',
                 'ticker': 's=',  # Exchange is added after ticker and '.'
                 'interval': 'g=',  # d, w, m, v: (daily, wkly, mth, dividends)
                 'start_date': 'a=00&b=1&c=1900',  # The entire price history
                 'end_date': yahoo_end_date,  # Today's date (MM; D; YYYY)
                 'csv': 'ignore=.csv'}  # Returns a CSV file

########################################################################################################################
#                                           MAIN PROCEDURE

"""
args ={
        'daily_downloads': ['yahoo'],#['quandl', 'yahoo', 'google'],
        'minute_downloads': []
       }
"""


class LoadPriceTable(luigi.Task):
    # priority = 40

    data_type = DATA_TYPE_STOCK
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)
    PATH_CSV_TABLES = luigi.configuration.get_config().get('table', 'PATH_CSV_TABLES', None)

    daily_downloads = luigi.Parameter()
    minute_downloads = luigi.Parameter()

    task_complete = False

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def run(self):
        args = {
            'daily_downloads': str(self.daily_downloads).split(','),
            'minute_downloads': str(self.minute_downloads).split(',')
        }
        download_list = build_download_list(args)
        if download_list:
            for source in download_list:
                if source['interval'] == 'daily':
                    table = 'daily_prices'
                    if source['source'] == 'google':
                        google_fin_url['interval'] = 'i=' + str(60 * 60 * 24)
                    elif source['source'] == 'yahoo':
                        yahoo_fin_url['interval'] = 'g=d'
                elif source['interval'] == 'minute':
                    table = 'minute_prices'
                    if source['source'] == 'google':
                        google_fin_url['interval'] = 'i=' + str(60)
                    elif source['source'] == 'yahoo':
                        logger.error('Yahoo Finance does not provide minute data.')
                        continue
                else:
                    logger.error(
                        'No interval was provided for %s in data_download in pySecMaster.py' % source['interval'])
                    continue

                if source['source'] == 'alpaca':
                    # Download data for selected Alpaca codes
                    logger.info('\nDownloading all Alpaca fields for: %s'
                                '\nNew data will %s the data' %
                                (source['selection'], source['data_process']))

                    # alpaca_prices = AlpacaDataExtraction(download_selection=source['selection'], table_name=table)
                    # alpaca_prices.main()

                if source['source'] == 'yahoo':
                    # Download data for selected Google Finance codes
                    logger.info('Downloading all Yahoo Finance fields for: %s... New data will %s the data' %
                                (source['selection'], source['data_process']))

                    yahoo_prices = YahooFinanceDataExtraction(download_selection=source['selection'], table_name=table)
                    yahoo_prices.main()


                elif source['source'] == 'google':
                    # Download data for selected Google Finance codes
                    logger.info('\nDownloading all Google Finance fields for: %s'
                                '\nNew data will %s the prior %s day\'s data' %
                                (source['selection'], source['data_process'],
                                 source['replace_days_back']))
                    pass
                elif source['source'] == 'quandl':
                    if QUANDL_API:
                        # Download data for selected Quandl codes
                        logger.info('\nDownloading all Quandl fields for: %s'
                                    '\nNew data will %s the prior %s day\'s data' %
                                    (source['selection'], source['data_process'],
                                     source['replace_days_back']))
                        # NOTE: Quandl only allows a single concurrent download with
                        #   their free account
                        pass
                    else:
                        logger.info('\nNot able to download Quandl data for %s because '
                                    'there was no Quandl API key provided.' %
                                    (source['selection'],))
                else:
                    logger.info('The %s source is currently not implemented. Skipping it.' %
                                source['source'])

            logger.info('All available data values have been downloaded for: %s' %
                        download_list)

        self.task_complete = True


######################################################################################################################


class PriceDataExtraction(ABC):

    def __init__(self, download_selection, table_name, vendor_name='Yahoo_Finance'):
        self.download_selection = download_selection.split('/')
        self.vendor_name = vendor_name
        self.table_name = table_name

        self.history = "max"
        self.vendor = None
        self.exchanges_df = None
        self.symbols_list = None

        self.latest_prices = None
        self.table_class = None
        self.interval = "1d"
        self.symbols_to_download=None

    def main(self):
        """
        The main PRice DataExtraction method is used to execute
        subsequent methods in the correct order.
        """
        start_time = time.time()

        asyncio.run(self.get_stocks())

        print('The price extraction from %s took %0.2f seconds to complete' %
              (self.vendor_name, time.time() - start_time))

    async def initalize(self):
        """
        :param download_selection: String indicating what selection of codes
            should be downloaded from Yahoo
        :param table_name: String indicating which table the DataFrame should be
            put into.
        :param vendor_name: Data vendor name
        """
        self.vendor = await Data_Vendors.get_vendor_by_name(vendor_name=self.vendor_name)
        self.vendor = self.vendor.to_dict('index')[0]

        # Build a DataFrame with all the exchange symbols
        self.exchanges_df = await Exchanges.get_all()

        logger.info('%s - Analyzing the symbols codes that will be downloaded...' % self.vendor['name'])
        # Create a list of tsids to download
        self.symbols_list = await self.get_symbols()  # self.download_selection, self.exchanges_df, self.vendor)
        self.symbols_list.set_index(['symbol_id'], inplace=True)

        logger.info('Retrieving dates of the last bar price for %s tickers...' % self.vendor['name'])

        if self.table_name == 'daily_prices':
            self.latest_prices = await Daily_LastPrices.get_by_vendor(vendor_id=int(self.vendor['data_vendor_id']))
            self.table_class = Daily_Prices
        elif self.table_name == 'minute_prices':
            self.latest_prices = None  # Minute_Prices.get_last_price(table=self.table, vendor_id=self.vendor_id)
            self.table_class = None

        logger.info('%s - Defining the list of symbols to download...' % self.vendor['name'])
        # Get DF of selected codes plus when (if ever) they were last updated
        symbols_df = self.symbols_list.join(self.latest_prices.set_index(['symbol_id']))
        symbols_df['date'] = symbols_df['date'].where(pd.notnull(symbols_df['date']), None)
        symbols_df['date'] = symbols_df['date'].replace({np.nan: None})
        symbols_df = symbols_df.reset_index()
        # Sort the DF with un-downloaded items first, then based on last update.
        # df.sort_values introduced in 0.17.0
        self.symbols_to_download = symbols_df.sort_values(['date', 'source_id'], axis=0,
                                                          ascending=[False, True], na_position='last')
#                                                          ascending=[True, True], na_position='first')

    async def get_symbols(self):  # , download_selection, exchanges_df, vendor):
        """ Builds a DataFrame of symbol_id from a SQL query. These codes are the
        items that will have their data downloaded.
        """
        ids = []
        for selection in self.download_selection:
            if selection == 'all':
                # Retrieve all symbol id from tickers
                tickers = await Tickers.get_active_symbols()
                tickers_ids = tickers['symbol_id'].to_list()
                ids.append(tickers_ids)
            elif selection == 'italy':
                exch_list = ['MIL']
                exchange_id = self.exchanges_df.loc[self.exchanges_df['symbol'].isin(exch_list)][
                    'exchange_id'].to_list()
                tickers = await Tickers.get_active_symbols(exchange_list=exchange_id)
                tickers_ids = tickers['symbol_id'].to_list()
                ids.extend(tickers_ids)
            elif selection == 'us_main':
                exch_list = ['AMEX', 'NYSE', 'NASDAQ']
                exchange_id = self.exchanges_df.loc[self.exchanges_df['symbol']
                              .isin(exch_list)]['exchange_id'].to_list()
                tickers = await Tickers.get_active_symbols(exchange_list=exchange_id)
                tickers_ids = tickers['symbol_id'].to_list()
                ids.extend(tickers_ids)

        ids = list(set(ids))
        symbols = await Symbology.get_all_by_symbol_id(ids, self.vendor['source'])
        return symbols

    async def load_data(self, data, start):
        ###
        # Gestione commit e rollback
        # https://docs.sqlalchemy.org/en/14/orm/session_basics.html#framing-out-a-begin-commit-rollback-block
        symbol = data['symbol'].iloc[0]
        logger.info("Starting writing %s record on Database for %s" % (len(data), symbol))
        # data.reset_index(inplace=True)
        data.drop_duplicates(inplace=True, ignore_index=True)

        if len(data) > 0:
            data = data.apply(lambda x: self.table_class.round_bar(x), axis=1)
            insert_data = data.loc[data['date'] > start]
            update_data = data.loc[data['date'] <= start]

            if not insert_data.empty:
                try:
                    await self.write_to_db(insert_data)
                except Exception as e:
                    #   session.rollback()
                    logger.info('Job: INSERT Data on db - Something went wrong: %s' % self.table_name.upper())
                    logger.error(str(e))
            if not update_data.empty:
                try:
                    await self.update_to_db(update_data)
                except Exception as e:
                    #   session.rollback()
                    logger.info('Job: UPLOAD Data on db - Something went wrong: %s' % self.table_name.upper())
                    logger.error(str(e))

            logger.info('Finish load Data for %s from %s - %s records merged on %s table' %
                        (symbol, self.vendor['name'], len(data), self.table_name.upper()))
            del data

    async def write_to_db(self, insert_data):
        data = insert_data[['symbol_id', 'date', 'open', 'high', 'low', 'close', 'volume',
                                   'dividends', 'splits', 'data_vendor_id']]
        data = data.to_dict(orient='records')
        query = "INSERT INTO %s (symbol_id, date, open, high, low, close, volume, dividends, splits, data_vendor_id) " \
                "VALUES (:symbol_id, :date, :open, :high, :low, :close, :volume, :dividends, :splits, :data_vendor_id)"\
                % self.table_name
        await database.execute_many(query, data)
        del data

    async def update_to_db(self, update_data):
        data = update_data[['date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'splits']]
        data = data.to_dict(orient='records')
        query = "UPDATE %s SET open=:open, high=:high, low=:low, close=:close, volume=:volume, dividends=:dividends, " \
                "splits=:splits WHERE symbol_id = '%s' AND date = :date AND data_vendor_id = %s" % \
                (self.table_name, update_data['symbol_id'][0], update_data['data_vendor_id'][0])
        await database.execute_many(query, data)
        del data

    @abstractmethod
    async def get_stocks(self):
        pass

    @abstractmethod
    async def get_price(self, engine, rate_limiter, request_params):
        pass


######################################################################################################################


class YahooFinanceDataExtraction(PriceDataExtraction):

    def __init__(self, download_selection, table_name):
        super().__init__(download_selection, table_name,vendor_name='Yahoo_Finance')
        if table_name == 'daily_prices':
            self.interval = "1d"
        elif table_name == 'minute_prices':
            self.interval = "1m"

    async def get_stocks(self):
        # engine = get_async_database()
        await database.connect()
        await self.initalize()
        today_ = date.today()  # - datetime.timedelta(1)
        today_ = datetime(today_.year, today_.month, today_.day)

        symbol_urls = {}
        i = 0
        self.symbols_to_download.reset_index(drop=True, inplace=True)
        for idx, code in self.symbols_to_download.iterrows():
            # set start_day by last candle datetime in data
            start = code['date']
            end = today_
            history = self.history

            if start in nothing:
                start = date(1990, 1, 1)

            #  set up symbol params
            if '_' not in code['source_id']:
                symbol_urls[i] = {'id': code['symbol_id'],
                                  'symbol': code['source_id'],
                                  'data_vendor_id': self.vendor['data_vendor_id'],
                                  'interval': self.interval,
                                  'history': history,
                                  'start': start,
                                  'end': end}
                i += 1

        try:
            async with RateLimiter(rate_limit=int(200 / 60), concurrency_limit=1) as rate_limiter:
                ret = await asyncio.gather(
                    *(self.get_price(rate_limiter, symbol_urls[i]) for i in range(len(symbol_urls))))
            logger.info("Finalized all from YAHOO. Returned list of {} outputs.".format(len(ret)))
            del ret
        except Exception as e:
            logger.error("Something wrong on Price Download job from Yahoo... Skip it!")
            logger.error(str(e))

        await database.disconnect()

    #####

    async def get_price(self, rate_limiter, request_params):
        start = request_params['start']

        try:
            async with rate_limiter.throttle():
                logger.info("Getting price from YAHOO for {} ... ".format(request_params['symbol']))

                ticker = yq.Ticker(request_params['symbol'])
                resp = None
                while resp is None:
                    try:
                        resp = ticker.history(period=request_params['history'], interval=request_params['interval'])
                    except ConnectionError or OSError as e:
                        time.sleep(10)
                        resp = None

                if isinstance(resp, pd.DataFrame):
                    resp = resp.dropna()
                    resp.reset_index(inplace=True)
                    resp['adjclose'] = resp['adjclose'].replace('-Infinity', 0).replace('Infinity', 0)

                    if 'splits' not in resp.columns:
                        resp['splits'] = 0
                    elif resp.iloc[-1]['splits'] != 0 or resp.iloc[-2]['splits'] != 0 or resp.iloc[-3]['splits'] != 0:
                        start = None

                    if start not in nothing:
                        # Keep only new bars
                        resp = resp.loc[resp['date'] >= start]

                    resp['symbol_id'] = request_params['id']
                    resp['data_vendor_id'] = request_params['data_vendor_id']
                    if 'dividends' not in resp.columns:
                        resp['dividends'] = 0

                    if not resp.empty:
                        await self.load_data(resp, start)
                        # await load_data(engine, resp, Daily_Prices.__tablename__)
                        logger.info("Downloaded {} from {} to {}\n".format(request_params['symbol'],
                                                                           resp['date'].iloc[0].strftime("%d/%m/%Y"),
                                                                           resp['date'].iloc[-1].strftime("%d/%m/%Y")))
                        del resp
                        del ticker
                      #  await asyncio.sleep(0.1)

                else:
                    logger.info("Data Not Found for {} from Yahoo\n".format(request_params['symbol']))

        except Exception as e:
            logger.error("Unable to get price for {} from {}.".format(request_params['symbol'], self.vendor['name']))
            logger.error(str(e))
            pass


######################################################################################################################


class AlpacaDataExtraction(PriceDataExtraction):

    def __init__(self, download_selection, table_name):
        super().__init__(download_selection, table_name, vendor_name='Alpaca')
        if table_name == 'daily_prices':
            self.interval = "1Day"
        elif table_name == 'minute_prices':
            self.interval = "1Min"

        self.base_url = "https://data.alpaca.markets/v2/stocks/%s/bars"
        self.request_headers = {'APCA-API-KEY-ID': ALPACA_PAPER['API_KEY'],
                                'APCA-API-SECRET-KEY': ALPACA_PAPER['API_SECRET']}

    async def get_stocks(self):
        await database.connect()
        await self.initalize()
        # set end_date
        tz = pytz.timezone('US/Eastern')
        today_ = datetime.now(tz).date() - timedelta(1)
        today_ = today_.strftime('%Y-%m-%d')
        #     today_ = datetime.datetime(today_.year, today_.month, today_.day)

        symbol_params = {}
        i = 0
        self.symbols_to_download.reset_index(drop=True, inplace=True)
        for idx, code in self.symbols_to_download.iterrows():
            # set start_day by last candle datetime in data
            start = code['date']
            end = today_
            # set history param
            if start in nothing:
                start = "1990-01-01"
            else:
                start = start.strftime("%Y-%m-%d")

            #  set up symbol params
            symbol_params[i] = {'id': code['symbol_id'],
                                'symbol': code['source_id'],
                                'data_vendor_id': self.vendor['data_vendor_id'],
                                'timeframe': self.interval,
                                'start': start,
                                'end': end
                                }
            i += 1

        try:
            async with RateLimiter(rate_limit=int(200 / 60), concurrency_limit=1) as rate_limiter:
                ret = await asyncio.gather(*(self.get_price(rate_limiter, symbol_params[i])
                                             for i in range(len(symbol_params))))
            logger.info("Finalized all from ALPACA. Returned list of {} outputs.".format(len(ret)))
            del ret
        except Exception as e:
            logger.error("Something wrong on Price Download job from ALPACA... Skip it!")
            logger.error(str(e))

        await database.disconnect()

    #######

    async def get_price(self, rate_limiter, params):
        request_params = {
            'timeframe': params['timeframe'],
            'limit': 10000,
            'start': params['start'],
            'end': params['end']
        }
        start = params['start']
        try:
            async with rate_limiter.throttle():
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=self.base_url % params['symbol'], params=request_params,
                                           headers=self.request_headers) as response:
                        logger.info("Getting price from ALPACA for {} ... ".format(params['symbol']))
                        resp = await response.read()
                        resp = json.loads(resp)

                        if resp['bars'] not in nothing and len(resp['symbol']) > 0:
                          #  time.sleep(0.1)
                            df = pd.DataFrame(resp['bars'])
                            df = df.dropna()

                            df = df.rename(columns={'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close',
                                                    'v': 'volume'})

                            df['symbol_id'] = params['id']
                            df['symbol'] = params['symbol']
                            df['data_vendor_id'] = params['data_vendor_id']

                            if 'adjclose' not in df.columns:
                                df['adjclose'] = 0
                            if 'dividends' not in df.columns:
                                df['dividends'] = 0
                            if 'splits' not in df.columns:
                                df['splits'] = 0
                            elif df.iloc[-1]['splits'] != 0 or df.iloc[-2]['splits'] != 0 or df.iloc[-3]['splits'] != 0:
                                start = None

                            if start not in nothing:
                                # Keep only new bars
                                df = df.loc[df['date'] >= start]

                            #   df['date'] = df['date'].apply(lambda x: datetime.date.fromisoformat(x.split('T')[0]))
                            df['date'] = df['date'].apply(
                                lambda x: datetime.strptime(x.split('T')[0], "%Y-%m-%d").date())

                            await self.load_data(df, datetime.strptime(start, "%Y-%m-%d").date())

                            logger.info("Downloaded {} from {} to {}\n"
                                        .format(params['symbol'], df['date'].iloc[0].strftime("%d/%m/%Y"),
                                                df['date'].iloc[-1].strftime("%d/%m/%Y")))
                            del df
                        else:
                            logger.info("Data Not Found for {} from ALPACA\n".format(params['symbol']))
                        del resp

        except Exception as e:
            logger.error("Unable to get price for {} from {}.".format(params['symbol'], self.vendor['name']))
            logger.error(str(e))

        del request_params
