import pandas as pd
import time

from settings.settings import logger
from models.base import Exchanges, Symbology
from api.db_api import database
from api.alpaca_api import alpacaAPI

from settings.config import CSI_TSID_EXCHANGE_LIST, CSI_TSID_SUB_EXCHANGE_LIST

'''
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''


def altered_values(existing_df, new_df):
    """ Compare the two provided DataFrames, returning a new DataFrame that only
    includes rows from the new_df that are different from the existing_df.

    :param existing_df: DataFrame of the existing values
    :param new_df: DataFrame of the next values
    :return: DataFrame with the altered/new values
    """

    # Convert both DataFrames to all string objects. Normally, the symbol_id
    #   column of the existing_df is an int64 object, messing up the merge
    if len(existing_df.index) > 0:
        existing_df = existing_df.applymap(str)
    else:
        existing_df = pd.DataFrame(columns=['symbol_id', 'source_id', 'is_active'])

    new_df = new_df.applymap(str)

    # DataFrame with the similar values from both the existing_df and the
    #   new_df. The comparison is based on the symbol_id/sid and
    #   source_id/ticker columns.
    combined_df = pd.merge(left=existing_df, right=new_df, how='inner',
                           left_on=['symbol_id', 'source_id', 'is_active'],
                           right_on=['sid', 'ticker', 'is_active'])

    # In a new DataFrame, only keep the new_df rows that did NOT have a match
    #   to the existing_df
    altered_df = new_df[~new_df['sid'].isin(combined_df['sid'])]

    return altered_df


async def create_symbology(input_df, source_list):
    """
    Create the symbology table. Use the CSI numbers as the unique symbol
    identifiers. See if they already exist within the symbology table, and if
    not, add them. For the source, use either 'csi_data' or the data vendor ID.
    For the source_id, use the actual CSI number.

    After the initial unique symbols are created, map the Quandl codes to their
    respective symbol ID. This can be done using the ticker and exchange
    combination, as it will always be unique (for active tickers...). For
    Quandl codes that don't have a match, create a unique symbol ID for them
    in a seperate number range (1M - 2M; B#). Use this same matching structure
    for mapping basket items.

    Steps:
    1. Download the CSI stockfacts data and store in database
    2. Add the CSI data's number ID to the symbology table if it hasn't been
        done yet
    3. Map Quandl codes to a unique ID

    :param
    input_df: Dataframe
    source_list: List of strings with the symbology sources to use
    """
    await database.connect()

    all_bi_stock_df = pd.DataFrame()
    all_csi_stock_df = pd.DataFrame()
    for key, item in input_df.items():
        if key == 'stock_csi_data':
            all_csi_stock_df = item[['csi_number', 'symbol', 'exchange', 'sub_exchange', 'is_active']].copy()
            all_csi_stock_df.rename(columns={'csi_number': 'sid', 'symbol': 'ticker'}, inplace=True)
        elif 'borsa_italiana' in key:
            all_bi_stock_df = pd.concat([all_bi_stock_df, item])

    all_bi_stock_df = all_bi_stock_df[['codice_isin', 'codice_alfanumerico']].copy()
    all_bi_stock_df.columns = ['sid', 'ticker']
    all_bi_stock_df['exchange'] = 'MIL'
    all_bi_stock_df['sub_exchange'] = ''
    all_bi_stock_df['is_active'] = 1

    all_csi_stock_df.dropna(subset=['ticker'], inplace=True)
    all_bi_stock_df.dropna(subset=['ticker'], inplace=True)

    exch_df = await Exchanges.get_all()

    # ToDo: Add economic_events codes
    data = pd.DataFrame()
    for source in source_list:

        logger.info('Processing the symbology IDs for %s' % (source,))
        source_start = time.time()

        # Retrieve any existing ID values from the symbology table
        existing_symbology_df = await Symbology.get_by_source(source, 'stock')
        altered_df = pd.DataFrame()

        if source == 'csi_data':
            # csi_stock_df = CSI_Stock.get_all(to_df=True)
            # csi_stock_df = all_csi_stock_df[['sid', 'ticker', 'exchange', 'sub_exchange','is_active']]
            csi_stock_df = all_csi_stock_df.sort_values('sid', axis=0)
            csi_stock_df.drop_duplicates(subset=['ticker', 'exchange', 'sub_exchange'], inplace=True)
            csi_stock_df.reset_index(drop=True, inplace=True)

            # csi_data is unique where the sid (csi_num) is the source_id
            csi_stock_df['ticker'] = csi_stock_df['sid']

            # Find the values that are different between the two DataFrames
            altered_values_df = altered_values(
                existing_df=existing_symbology_df, new_df=csi_stock_df)

            # Prepare a new DataFrame with all relevant data for these values
            altered_df = pd.DataFrame()
            altered_df.insert(0, 'id', altered_values_df['sid'] + "_" + source)
            altered_df.insert(1, 'symbol_id', altered_values_df['sid'])
            altered_df.insert(2, 'source', source)
            altered_df.insert(3, 'source_id', altered_values_df['sid'])
            altered_df.insert(4, 'is_active', altered_values_df['is_active'])
            altered_df.insert(5, 'type', 'stock')

        elif source == 'bi_data':
            italia_df = all_bi_stock_df.copy()
            italia_df.reset_index(drop=True, inplace=True)

            # csi_data is unique where the sid (csi_num) is the source_id
            italia_df['ticker'] = italia_df['sid']

            # Find the values that are different between the two DataFrames
            altered_values_df = altered_values(
                existing_df=existing_symbology_df, new_df=italia_df)

            # Prepare a new DataFrame with all relevant data for these values
            altered_df = pd.DataFrame()
            altered_df.insert(0, 'id', altered_values_df['sid'] + "_" + source)
            altered_df.insert(1, 'symbol_id', altered_values_df['sid'])
            altered_df.insert(2, 'source', source)
            altered_df.insert(3, 'source_id', altered_values_df['sid'])
            altered_df.insert(4, 'is_active', altered_values_df['is_active'])
            altered_df.insert(5, 'type', 'stock')

        elif source in ['tsid', 'quandl_wiki', 'quandl_eod', 'quandl_goog', 'seeking_alpha', 'yahoo', 'alpaca']:
            # These sources have a similar symbology creation process
            if source == 'tsid':
                # Build tsid codes (<ticker>.<exchange>.<position>), albeit
                #   only for American, Canadian and London exchanges.

                # Restricts tickers to those that are traded on exchanges only
                #   (AMEX, LSE, MSE, NYSE, OTC (NASDAQ, BATS), TSX, VSE). For
                #   the few duplicate tickers, choose the active one over the
                #   non-active one (same company but different start and end
                #   dates, with one being active).
                # NOTE: Due to different punctuations, it is possible for
                #   items with similar symbol, exchange and sub exchange
                #   to be returned (ie. 'ABK.A TSX' and 'ABK+A TSX')

                # ITALIAN TICKERS
                bi_stock_df = all_bi_stock_df.copy()

                def bi_to_tsid(row):
                    # Create the tsid symbol combination of:
                    #   <ticker>.<tsid exchange symbol>.<count>
                    ticker = row['ticker']
                    exchange = row['exchange']

                    # Either no sub exchange or the sub exchange
                    #   never found a match
                    tsid_exch = (exch_df.loc[exch_df['symbol'] ==
                                             exchange, 'tsid_symbol'].values)
                    if tsid_exch:
                        return ticker + '.' + tsid_exch[0] + '.0'
                    else:
                        logger.info('Unable to find the tsid exchange symbol '
                                    'for the exchange %s in bi_to_tsid' % exchange)

                bi_stock_df['ticker'] = bi_stock_df.apply(bi_to_tsid, axis=1)

                # AMERICAN TICKERS
                # csi_stock_df = CSI_Stock.get_stocks_by_exchanges(exchange=CSI_TSID_EXCHANGE_LIST,
                # sub_exchange=CSI_TSID_SUB_EXCHANGE_LIST)
                csi_stock_df = all_csi_stock_df.loc[(all_csi_stock_df['exchange'].isin(CSI_TSID_EXCHANGE_LIST)) | (
                    all_csi_stock_df['sub_exchange'].isin(CSI_TSID_SUB_EXCHANGE_LIST))].copy()

                csi_stock_df.sort_values(by=['ticker', 'exchange', 'is_active'],
                                         axis=0, ascending=[True, True, False], inplace=True)
                csi_stock_df.drop_duplicates(subset=['ticker', 'exchange'], inplace=True)
                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.+-]', value=r'_')

                def csi_to_tsid(row):
                    # Create the tsid symbol combination of:
                    #   <ticker>.<tsid exchange symbol>.<count>
                    ticker = row['ticker']
                    exchange = row['exchange']
                    sub_exchange = row['sub_exchange']

                    if sub_exchange == 'NYSE ARCA':
                        # NYSE ARCA is a special situation where the sub
                        #   exchange matches the csi_symbol
                        tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                                 sub_exchange, 'tsid_symbol'].values)
                        if tsid_exch:
                            return ticker + '.' + tsid_exch[0] + '.0'
                        else:
                            logger.info('Unable to find the tsid exchange symbol for '
                                        'the sub exchange %s in csi_to_tsid' % sub_exchange)
                    elif sub_exchange == 'NYSE Mkt':
                        # AMEX changed to NYSE Mkt, but only the sub exchange
                        #   from csi data is showing this, not the exchange.
                        #   Thus, the exchanges table will continue using AMEX.
                        tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                                 'AMEX', 'tsid_symbol'].values)
                        if tsid_exch:
                            return ticker + '.' + tsid_exch[0] + '.0'
                        else:
                            logger.info('Unable to find the tsid exchange symbol for '
                                        'the sub exchange NYSE Mkt in csi_to_tsid')
                    else:
                        # For all non NYSE ARCA exchanges, see if there is a
                        #   sub exchange and if so, try matching that to the
                        #   csi_symbol (first) or name (second). If no sub
                        #   exchange, try matching to the csi_symbol.
                        if sub_exchange:
                            # (exch: AMEX | chld_exch: NYSE)
                            tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                                     sub_exchange, 'tsid_symbol'].values)
                            if tsid_exch:
                                return ticker + '.' + tsid_exch[0] + '.0'
                            else:
                                # (exch: NYSE | chld_exch: OTC Markets QX)
                                # (exch: AMEX | chld_exch: BATS Global Markets)
                                tsid_exch = (exch_df.loc[exch_df['name'] ==
                                                         sub_exchange, 'tsid_symbol'].
                                             values)
                                if tsid_exch:
                                    return ticker + '.' + tsid_exch[0] + '.0'
                                else:
                                    logger.info('Unable to find the tsid exchange symbol for the sub exchange %s in '
                                                'csi_to_tsid. Will try to find a match for the exchange now.'
                                                % sub_exchange)
                                    # If there is an exchange, try to match that
                        if exchange:
                            # Either no sub exchange or the sub exchange
                            #   never found a match
                            tsid_exch = (exch_df.loc[exch_df['csi_symbol'] ==
                                                     exchange, 'tsid_symbol'].values)
                            if tsid_exch:
                                try:
                                    return ticker + '.' + tsid_exch[0] + '.0'
                                except Exception:
                                    logger.error("Errore estrazione TSID Exchange")
                            else:
                                logger.info('Unable to find the tsid exchange symbol '
                                            'for the exchange %s in csi_to_tsid' %
                                            exchange)
                        else:
                            logger.info('Unable to find the tsid exchange symbol for '
                                        'either the exchange or sub exchange for '
                                        '%s:%s' % (exchange, sub_exchange))

                csi_stock_df['ticker'] = csi_stock_df.apply(csi_to_tsid, axis=1)

                stock_df = pd.concat([csi_stock_df, bi_stock_df])

            elif source == 'yahoo':
                # ITALIAN TICKERS
                bi_stock_df = all_bi_stock_df.copy()

                # Imply plausible Yahoo codes, albeit only for American,
                #   Canadian and London exchanges.
                csi_stock_df = all_csi_stock_df.loc[(all_csi_stock_df['exchange'].isin(CSI_TSID_SUB_EXCHANGE_LIST))
                                                    | (all_csi_stock_df['sub_exchange'].isin(
                    CSI_TSID_SUB_EXCHANGE_LIST))].copy()
                csi_stock_df.sort_values(by=['ticker', 'exchange', 'is_active'], axis=0,
                                         ascending=[True, True, False], inplace=True)
                csi_stock_df.drop_duplicates(subset=['ticker', 'exchange', 'is_active'], inplace=True)

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.-]', value=r'-')
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[+]', value=r'-P')

                def csi_to_yahoo(row):
                    # Create the Yahoo symbol combination of <ticker>.<exchange>

                    ticker = row['ticker']
                    exchange = row['exchange']
                    sub_exchange = row['sub_exchange']
                    us_exchanges = ['AMEX', 'BATS Global Markets',
                                    'Nasdaq Capital Market',
                                    'Nasdaq Global Market',
                                    'Nasdaq Global Select',
                                    'NYSE', 'NYSE ARCA']

                    if exchange in ['AMEX', 'NYSE'] \
                            or sub_exchange in us_exchanges:
                        return ticker  # US ticker; no exchange needed
                    elif exchange == 'LSE':
                        return ticker + '.L'  # LSE -> L
                    elif exchange == 'TSX':
                        return ticker + '.TO'  # TSX -> TO
                    elif exchange == 'VSE':
                        return ticker + '.V'  # VSE -> V
                    elif sub_exchange == 'OTC Markets Pink Sheets':
                        return ticker  # + '.PK'   # OTC Pinks -> PK
                    elif exchange == 'MIL':
                        return ticker + '.MI'
                    else:
                        logger.info('csi_to_yahoo did not find a match for %s with an '
                                    'exchange of %s and a sub exchange of %s' %
                                    (ticker, exchange, sub_exchange))

                stock_df = pd.concat([csi_stock_df, bi_stock_df])
                stock_df['ticker'] = stock_df.apply(csi_to_yahoo, axis=1)

            elif source == 'alpaca':
                # Imply plausible Alpaca codes, only American exchanges.

                csi_stock_df = all_csi_stock_df.loc[(all_csi_stock_df['exchange'].isin(CSI_TSID_SUB_EXCHANGE_LIST))
                                                    | (all_csi_stock_df['sub_exchange'].isin(
                    CSI_TSID_SUB_EXCHANGE_LIST))].copy()

                csi_stock_df.sort_values(by=['ticker', 'exchange', 'is_active'], axis=0,
                                         ascending=[True, True, False], inplace=True)
                csi_stock_df.drop_duplicates(subset=['ticker', 'exchange', 'is_active'], inplace=True)

                # If a ticker has a ". + -", change it to an underscore
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[.UN]', value=r'.U')
                csi_stock_df['ticker'].replace(regex=True, inplace=True,
                                               to_replace=r'[+]', value=r'-')

                # Get list of symbols in Alpaca API
                alpaca_assets = alpacaAPI().get_assets()
                alpaca_symbols = [asset.symbol for asset in alpaca_assets]

                stock_df = csi_stock_df.loc[csi_stock_df['ticker'].isin(alpaca_symbols)]

            else:
                return logger.error('%s is not implemented in the '
                                    'create_symbology function of '
                                    'build_symbology.py' % source)

            # Remove post processed duplicates to prevent database FK errors
            stock_df = stock_df.drop_duplicates(subset=['ticker'])

            # Find the values that are different between the two DataFrames
            altered_values_df = altered_values(
                existing_df=existing_symbology_df, new_df=stock_df)

            # Prepare a new DataFrame with all relevant data for these values
            altered_df = pd.DataFrame()
            altered_df.insert(0, 'id', altered_values_df['sid'] + "_" + source)
            altered_df.insert(1, 'symbol_id', altered_values_df['sid'])
            altered_df.insert(2, 'source', source)
            altered_df.insert(3, 'source_id', altered_values_df['ticker'])
            altered_df.insert(4, 'is_active', altered_values_df['is_active'])
            altered_df.insert(5, 'type', 'stock')

        else:
            logger.error('%s is not implemented in the create_symbology function of build_symbology.py' % source)

        data = data.append(altered_df)
        """
        for index, row in altered_df.iterrows():
            try:
                # create records to add
                record = Symbology.process_values(row)
                data.append(record)
            except Exception:
                logger.error('Unable to create a record for %s table. Skipping it' % Symbology.__tablename__)
                logger.error(str(e))
                continue
        """
        logger.info('Finished processing the symbology IDs for %s taking %0.2f seconds'
                    % (source, (time.time() - source_start)))

    data.reset_index(drop=True, inplace=True)
    logger.info('Added all %i sources to the symbology table.' % (len(source_list)))
    await database.disconnect()
    return data
