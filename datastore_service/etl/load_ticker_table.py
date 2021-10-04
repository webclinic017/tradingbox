import time
import asyncio

import pandas as pd
import numpy as np

import luigi
from luigi import LocalTarget

from settings.config import DATA_TYPE_STOCK
from settings.settings import logger
from api.db_api import database

from models.base import Symbology, Exchanges, Asset_Types, SubExchanges, Scale, Sector_Types
from models.tickers import Tickers, Sectors, Currencies, Indices

from etl.etl_base import SqlTarget
from etl.etl_extrator import ExtractorCSIDataTask, ExtractorBorsaItalianaTask, DownloadXLSBorsaItalianaTask
from etl.etl_yahoo_extractor import ExtractorYahooDataTask
from etl.load_data_symbology import LoadDataSymbologyTask

from etl.utils import get_stocks_csi, get_etf, get_etn, get_index, get_mutual, clean_csi_df, clean_bi_df

from settings.settings import LUIGI_CONFIG_PATH

luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)


YAHOO = 'Yahoo'
BI = 'BorsaItaliana'
data_type = [DATA_TYPE_STOCK]

nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']


########################################################################################################################
#                                           MAIN PROCEDURE
"""
    def run(self):
        # Populate major infos of Ticker Table
        TickerExtractor(self.data_type)

        # Populate Indices table
        IndicesExtractor()

        logger.info('Complete TICKER EXTRACTOR process!')
"""

symbol_source = 'tsid'
# symbol_type = 'stock'

us_exchange = ['NASDAQ', 'NYSE', 'AMEX']


#################################
# GETTING INFO FOR SECTOR


def get_input_dict(list_input):
    results = []
    for inpt in list_input:
        if type(inpt) is LocalTarget:
            results.append(inpt)
        elif type(inpt) is list:
            for item in inpt:
                results.append(item)
    return results


class LoadTickerTable(luigi.Task):
 #   priority = 60

    data_type = DATA_TYPE_STOCK
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    csidata_type = DATA_TYPE_STOCK
    input_df = {}
    sector_type_df = None
    existing_sector_df = None
    existing_scale_df = None
    existing_currencies_df = None

    task_complete = False

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def requires(self):
        return [ExtractorCSIDataTask(self.csidata_type),
                ExtractorBorsaItalianaTask('azioni'),
                ExtractorBorsaItalianaTask('etf'),
                ExtractorBorsaItalianaTask('etc-etn'),
                ExtractorBorsaItalianaTask('fondi'),
                ExtractorBorsaItalianaTask('indici'),
                DownloadXLSBorsaItalianaTask(),
                ExtractorYahooDataTask()]

    def run(self):
        input_value = get_input_dict(self.input())
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                in_file = table_in.path
                in_name = in_file.replace(self.data_dir + "/", "").replace(".csv", "")
                self.input_df[in_name] = pd.read_csv(in_file, header=0, sep='\t', keep_default_na=False, na_values=[''])
                #   self.input_df[in_name] = self.input_df[in_name].where(self.input_df[in_name].notnull(), None)
                self.input_df[in_name] = self.input_df[in_name].replace({np.nan: None})

        asyncio.run(self.populate_tickers())

        self.task_complete = True

    def output(self):
        return SqlTarget()

    ##############################################################################################################
    #       POPULATE TICKERS
    #
    async def populate_tickers(self):
        await database.connect()
        logger.info('Processing the TICKER Creation job... ')
        start_time = time.time()
        #
        #       BASE ARCHIVIE
        #
        # Get exchanges data
        exchange_df = await Exchanges.get_all()
        exchange_id = exchange_df.set_index('symbol').to_dict()['exchange_id']

        # Get sub_exchanges data
        sub_exchange_df = await SubExchanges.get_all()
        sub_exchange_id = sub_exchange_df.set_index('subexchange_name').to_dict()['subexchange_id']

        # Get asset_type data
        asset_type_df = await Asset_Types.get_all()
        asset_type_id = asset_type_df.set_index('asset_type_code').to_dict()['asset_type_id']

        # Get Symbology
        symbol_df = await Symbology.get_by_source(symbol_source, 'stock')
        symbol_df.drop(['is_active'], axis=1, inplace=True)
        symbol_df.set_index(['symbol_id'], inplace=True)

        # Retrive existing Ticker
        existing_ticker_df = await Tickers.get_all()
        if not existing_ticker_df.empty:
            existing_ticker_df = existing_ticker_df.loc[existing_ticker_df['is_active'] == True]

        # Get SECTOR TYPES
        self.sector_type_df = await Sector_Types.get_all()
        # Get SECTORS
        self.existing_sector_df = await Sectors.get_all()
        # Get MARKET CAP SCALE
        self.existing_scale_df = await Scale.get_all()
        # get CURRENCIES
        self.existing_currencies_df = await Currencies.get_all()

        await database.disconnect()
        ##############################################
        #       DATA ARCHIVIE
        #
        # Getting basic info from archivie of CSI data and Borsa Italiana
        try:
            # Get stock csi data
            stock_df = clean_csi_df(get_stocks_csi(self.input_df['stock_csi_data']), asset_type_id['stock'])
            etf_df = clean_csi_df(get_etf(self.input_df['stock_csi_data']), asset_type_id['etf'])
            etn_df = clean_csi_df(get_etn(self.input_df['stock_csi_data']), asset_type_id['etn'])
            index_df = clean_csi_df(get_index(self.input_df['stock_csi_data']), asset_type_id['index'])
            mutual_df = clean_csi_df(get_mutual(self.input_df['stock_csi_data']), asset_type_id['mutual'])
            # create dataframe with all ticker
            csi_df = pd.concat([stock_df, etf_df, etn_df, index_df, mutual_df], ignore_index=True)
            csi_df = csi_df.astype(str)

            # Get stock borsa italiana data
            bi_stock_df = clean_bi_df(self.input_df['azioni_borsa_italiana'], asset_type_id['stock'])
            bi_etf_df = clean_bi_df(self.input_df['etf_borsa_italiana'], asset_type_id['etf'])
            bi_etn_df = clean_bi_df(self.input_df['etc-etn_borsa_italiana'], asset_type_id['etn'])
            bi_mutual_df = clean_bi_df(self.input_df['fondi_borsa_italiana'], asset_type_id['mutual'])
            bi_index_df = clean_bi_df(self.input_df['indici_borsa_italiana'], asset_type_id['index'])
            # create dataframe with all ticker
            bi_df = pd.concat([bi_stock_df, bi_etf_df, bi_etn_df, bi_index_df, bi_mutual_df], ignore_index=True)
            bi_df = bi_df.astype(str)

            df = pd.concat([bi_df, csi_df], ignore_index=True)
            # df = bi_df.copy()

        except Exception as e:
            logger.info('Unable to load symbols from Database. Skipping it')
            logger.error(str(e))
            return

        #############################################################################
        #           CREATE TICKER

        try:
            # Prepare a new DataFrame with all relevant data for these values
            ticker_df = df.copy()  # pd.DataFrame()
            ticker_df.set_index(['sid'], inplace=True)
            ticker_df['is_active'] = np.where(ticker_df['is_active'].astype(int) == 1, True, False)

            ticker_df['exchange_id'] = ticker_df['exchange'].apply(
                lambda x: exchange_id[x] if x not in nothing else None)
            ticker_df['sub_exchange_id'] = ticker_df['sub_exchange'].apply(
                lambda x: sub_exchange_id[x] if x not in nothing else None)

            ticker_df = ticker_df.join(symbol_df)

            ticker_df.reset_index(inplace=True)
            ticker_df = ticker_df.rename(
                columns={'sid': 'symbol_id', 'ticker': 'symbol', 'id': 'tsid', 'asset_id': 'asset_type_id'})

            # Drop ticker without Symbology and format as integer
            ticker_df.dropna(subset=['tsid'], inplace=True)
            ticker_df.drop(['exchange', 'sub_exchange', 'source_id'], axis=1, inplace=True)
        #    ticker_df = ticker_df.where(pd.notnull(ticker_df), None)
        #    ticker_df.to_csv("tickers.csv", sep="\t")
        except Exception as e:
            logger.info('Unable to create Ticker Dataframe. Something wrong. Skipping it')
            logger.error(str(e))
            return

        ######################################################
        #       CREATE SECTORS FOR TICKERS
        sector_id = await self.create_sector_id()

        ######################################################
        #       CREATE MARKETCAP SCALE FOR TICKERS
        marketcap_id = await self.create_marketcap_scale_id()

        ######################################################
        #       SET CURRENCY FOR TICKERS
        currency_id = await self.create_currency_id()

        logger.info("Starting to Update TICKER table...")
        try:
            ticker_df.set_index(['symbol_id'], inplace=True)
            # Update Ticker with all relevant data for these values
            ticker_df = ticker_df.join(sector_id)
            ticker_df = ticker_df.join(marketcap_id)
            ticker_df = ticker_df.join(currency_id)

            #  ticker_df = ticker_df.where(pd.notnull(ticker_df), None)
            ticker_df = ticker_df.replace({np.nan: None})

            ticker_df.reset_index(inplace=True)
        except Exception as e:
            logger.info('Unable to create Ticker Dataframe. Something wrong. Skipping it')
            logger.error(str(e))
            return

        await self.output().load_data(data=ticker_df, table_name=Tickers.tablename)
        ######################################################
        #       UPDATE TICKERS
        #
        # Select the active Ticker
        if not existing_ticker_df.empty:
            active_df = ticker_df.loc[ticker_df['is_active'] == True].copy()

            # Find the values that are different between the two DataFrames
            not_active_df = existing_ticker_df[~existing_ticker_df['symbol_id'].isin(active_df['symbol_id'])].copy()
            not_active_df['is_active'] = False

            # REPLACE NaN VALUES
            # ticker_df = ticker_df.replace({np.nan: None})
            not_active_df = not_active_df.replace({np.nan: None})
            # logger.info('List of all record for TICKER table created. Ready for merging')
            await self.output().load_data(data=not_active_df, table_name=Tickers.tablename)

        logger.info('Tickers Populated from Yahoo in %0.1f seconds' % (time.time() - start_time))

    #############################################
    async def create_sector_id(self):
        start_time = time.time()
        # Get Yahoo Data for Sectors
        yahoo_df = self.input_df['summaryProfile_yahoo_data'].copy()
        yahoo_df = yahoo_df.loc[~yahoo_df['sector'].isnull()]
        yahoo_df['code'] = None

        # Get Borsa Italiana EFT ETC ETN for Sectors

        bi_etf_df = self.input_df['etf_borsa_italiana'][['codice_isin', 'valuta_denominazione']]
        bi_etn_df = self.input_df['etc-etn_borsa_italiana'][['codice_isin', 'valuta_denominazione']]
        bi_mutual_df = self.input_df['fondi_borsa_italiana'][['codice_isin', 'valuta_denominazione']]

        bi_all = pd.concat([bi_etf_df, bi_etn_df, bi_mutual_df])
        bi_all.set_index(['codice_isin'], inplace=True)

        bi_df = (self.input_df['XLS_borsa_italiana']).copy()
        bi_df.set_index(['ISIN'], inplace=True)
        bi_df = bi_df.join(bi_all)

        bi_df['valuta_denominazione'] = bi_df['valuta_denominazione'].replace({np.nan: None})
        bi_df[['industry']] = None
        #   bi_df['symbol_id'] = bi_df.index
        bi_df.reset_index(inplace=True)
        bi_df = bi_df.rename(
            columns={'ISIN': 'symbol_id', 'AREA BENCHMARK': 'sector', 'valuta_denominazione': 'code'})

        # Retrieve any existing values from the Sector table
        if not self.existing_sector_df.empty:
            existing_sector_df = self.existing_sector_df.drop(['sector_id', 'dt_created', 'dt_updated'], axis=1)
        else:
            existing_sector_df = pd.DataFrame()

        sector_type_df = self.sector_type_df

        async def populate_sectors(data, source=None):
            # Get Sector Type
            # Retrive existing Ticker

            sector_type_row = sector_type_df.loc[sector_type_df['sector_type_name'] == source]
            sector_type_id = sector_type_row.iloc[0]['sector_type_id']

            # Get unique sector and industry froom Yahoo
            new_df = data[['sector', 'industry', 'code']]
            new_df = new_df.drop_duplicates(keep="first")
            new_df = new_df.sort_values(by='sector', na_position='first')
            new_df.dropna(subset=['sector'], inplace=True)

            # DataFrame with the similar values from both the existing_df and the
            #   new_df. The comparison is based on the sector and industry columns.
            new_df['type_id'] = sector_type_id
            if existing_sector_df.empty:
                altered_df = new_df
            else:
                combined_df = new_df.merge(existing_sector_df, on=['sector', 'industry', 'code', 'type_id'])
                # In a new DataFrame, only keep the new_df rows that did NOT have a match
                #   to the existing_df
                altered_df = new_df[(~new_df['sector'].isin(combined_df['sector'])) &
                                    (~new_df['industry'].isin(combined_df['industry'])) &
                                    (~new_df['code'].isin(combined_df['code'])) &
                                    (~new_df['type_id'].isin(combined_df['type_id']))]

            await self.output().load_data(data=altered_df, table_name=Sectors.tablename)
            return sector_type_id

        # Get Sectors data of CSI data
        sector_type_csi = await populate_sectors(yahoo_df, source=YAHOO)

        # Get Sectors data of Borsa Italiana
        sector_type_bi = await populate_sectors(bi_df, source=BI)

        await database.connect()
        result_df = await Sectors.get_all()
        await database.disconnect()

        sector_database_csi = result_df.loc[result_df['type_id'] == sector_type_csi]
        sector_database_csi = sector_database_csi[['sector_id', 'sector', 'industry', 'code']]

        sector_database_bi = result_df.loc[result_df['type_id'] == sector_type_bi]
        sector_database_bi = sector_database_bi[['sector_id', 'sector', 'industry', 'code']]

        # Merge sector database
        sector_database = pd.concat([sector_database_csi, sector_database_bi], ignore_index=True)

        sector_df = pd.concat([yahoo_df[['symbol_id', 'sector', 'industry', 'code']],
                               bi_df[['symbol_id', 'sector', 'industry', 'code']]],
                              ignore_index=True)

        def get_sector_id(sector, industry, code=None):
            idx = None
            sector_row = sector_database.loc[sector_database['sector'] == sector]
            if industry:
                sector_row = sector_row.loc[sector_database['industry'] == industry]
            if code:
                sector_row = sector_row.loc[sector_database['code'] == code]
            if not sector_row.empty:
                idx = int(sector_row['sector_id'].iloc[0])
            return idx

        sector_df['sector_id'] = sector_df.apply(lambda x: get_sector_id(x['sector'], x['industry'], x['code']), axis=1)
        sector_df.set_index(['symbol_id'], inplace=True)
        sector_df = sector_df[['sector_id']]
        logger.info('SECTORS Table: Updated and getting IDs | %0.2f seconds' % (time.time() - start_time))
        return sector_df

    ############################################
    async def create_marketcap_scale_id(self):
        start_time = time.time()
        # Get Yahoo Data for MarketCap
        yahoo_df = self.input_df['summaryDetail_yahoo_data'].copy()
        yahoo_df = yahoo_df.loc[~yahoo_df['marketCap'].isnull()]

        marketcap_database = self.existing_scale_df
        marketcap_df = yahoo_df[['symbol_id', 'marketCap']].copy()

        def get_scale_id(marketcap_value):
            idx = 0
            if marketcap_value:
                value = int(float(marketcap_value))
                marketcap_row = marketcap_database.loc[(marketcap_database['min_value'] <= value) &
                                                       (marketcap_database['max_value'] > value)]
                if not marketcap_row.empty:
                    idx = marketcap_row['scale_id'].iloc[0]
            return idx

        marketcap_df['market_cap_scale_id'] = marketcap_df.apply(lambda x: get_scale_id(x['marketCap']), axis=1)
        marketcap_df.set_index(['symbol_id'], inplace=True)
        marketcap_df = marketcap_df[['market_cap_scale_id']]

        logger.info('MARKET CAP SCALE Table: Getting IDs | %0.2f seconds' % (time.time() - start_time))
        return marketcap_df

    ###########################################

    async def create_currency_id(self):
        start_time = time.time()
        # Get Currencies data
        # yahoo_df = Yahoo_summaryDetail.get_all()
        yahoo_df = (self.input_df['summaryDetail_yahoo_data']).copy()
        yahoo_df['currency'] = yahoo_df['currency'].str.upper()

        existing_curr_df = self.existing_currencies_df

        async def populate_currency(data):

            # Get currency froom Yahoo
            new_df = pd.DataFrame(data['currency'].str.upper())
            new_df.columns = ['currency_code']
            new_df = new_df.drop_duplicates(keep="first", subset='currency_code')
            new_df = new_df.sort_values(by='currency_code', na_position='first')
            #      new_df = new_df.loc[data['currency'].notnull()]

            # DataFrame with the similar values from both the existing_df and the
            #   new_df. The comparison is based on the sector and industry columns.
            if not existing_curr_df.empty:
                combined_df = new_df.merge(existing_curr_df.drop(['currency_id', 'dt_created', 'dt_updated'], axis=1),
                                           on=['currency_code'])
                # In a new DataFrame, only keep the new_df rows that did NOT have a match
                #   to the existing_df
                altered_df = new_df[~new_df['currency_code'].isin(combined_df['currency_code'])]
            else:
                altered_df = new_df

            await self.output().load_data(data=altered_df, table_name=Currencies.tablename)

        await populate_currency(yahoo_df)

        await database.connect()
        currency_database = await Currencies.get_all()
        await database.disconnect()
        currency_df = yahoo_df[['symbol_id', 'currency']].copy()

        def get_currency_id(currency_code):
            idx = None
            row = currency_database.loc[currency_database['currency_code'] == currency_code]
            if not row.empty:
                idx = int(row['currency_id'].iloc[0])
            return idx

        currency_df['currency_id'] = currency_df.apply(lambda x: get_currency_id(x['currency']), axis=1)
        currency_df.set_index(['symbol_id'], inplace=True)
        currency_df = currency_df[['currency_id']]

        logger.info('CURRENCY Table: Updated and getting IDs | %0.2f seconds' % (time.time() - start_time))
        return currency_df

######################################################
#       SET INDICES FOR TICKERS


class LoadIndicesTable(luigi.Task):
  #  priority = 50

    data_type = DATA_TYPE_STOCK
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    input_df = {}
    task_complete = False

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def requires(self):
        return [ExtractorBorsaItalianaTask('azioni')]

    def output(self):
        return SqlTarget()

    def run(self):
        logger.info('Processing the INDICES Creation job... ')
        start_time = time.time()

        input_value = get_input_dict(self.input())
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                in_file = table_in.path
                in_name = in_file.replace(self.data_dir + "/", "").replace(".csv", "")
                self.input_df[in_name] = pd.read_csv(in_file, header=0, sep='\t', keep_default_na=False, na_values=[''])
                # self.input_df[in_name] = self.input_df[in_name].where(self.input_df[in_name].notnull(), None)
                self.input_df[in_name] = self.input_df[in_name].replace({np.nan: None})

        asyncio.run(self.get_indices())

        logger.info('INDICES in %0.1f seconds' % (time.time() - start_time))
        self.task_complete = True

    async def get_indices(self):
        bi_stocks = self.input_df['azioni_borsa_italiana']

        # Create Indices dataframe
        indices_df = pd.DataFrame()
        for idx, row in bi_stocks.iterrows():
            idticker = row['codice_isin']
            indici_list = row['indici']
            if indici_list not in nothing:
                indices = row['indici'].split(',')
                for index in indices:
                    indices_df = indices_df.append({'ticker_id': idticker, 'index_id': index}, ignore_index=True)

        # Retrive existing Indices
        await database.connect()
        existing_indices_df = await Indices.get_all()
        await database.disconnect()

        if not existing_indices_df.empty:
            existing_df_active = existing_indices_df.loc[existing_indices_df['is_active'] == True].copy()
            existing_df_active = existing_df_active[['ticker_id', 'index_id']]
        else:
            existing_df_active = pd.DataFrame(columns=['ticker_id', 'index_id'])

        # Prepare a DataFrame with new data
        new_df = indices_df.merge(existing_df_active, how='left', indicator=True)
        new_df = new_df[new_df['_merge'] == 'left_only'].copy()
        new_df.drop(['_merge'], axis=1, inplace=True)
        new_df['is_active'] = True

        # Prepare a DataFrame with non active data, already present in database
        old_df = existing_df_active.merge(indices_df, how='left', indicator=True)
        old_df = old_df[old_df['_merge'] == 'left_only'].copy()
        old_df.drop(['_merge'], axis=1, inplace=True)
        old_df['is_active'] = False

        # load data on database
        await self.output().load_data(new_df, Indices.tablename)
        await self.output().load_data(old_df, Indices.tablename)
