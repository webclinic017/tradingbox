from datetime import datetime
import pandas as pd
import numpy as np
import time
from typing import List
from traceback import format_exc

from settings.settings import logger
from models.base import Exchanges, SubExchanges
from api.db_api import database


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
        existing_df = pd.DataFrame(columns=['subexchange_name', 'exchange_id'])

    new_df['id'] = np.arange(len(new_df.index))
    new_df = new_df.applymap(str)

    # DataFrame with the similar values from both the existing_df and the
    #   new_df. The comparison is based on the symbol_id/sid and
    #   source_id/ticker columns.
    combined_df = pd.merge(left=existing_df, right=new_df, how='inner',
                           left_on=['subexchange_name', 'exchange_id'],
                           right_on=['sub_exchange', 'exchange_id'])

    # In a new DataFrame, only keep the new_df rows that did NOT have a match
    #   to the existing_df
    altered_df = new_df[~new_df['id'].isin(combined_df['id'])]

    return altered_df


async def create_subexchange(csi_stock_df, bi_stock_df, bi_fondi_df):
    """
    Create the subexchange table.
    """

    logger.info('Processing the sub_exchange...')
    await database.connect()
    start = time.time()

    # Retrieve any existing values from the sub-exchange table
    existing_df = await SubExchanges.get_all()

    # Retrieve any existing values from the exchange table
    exch_df = await Exchanges.get_all()

  #  csi_stock_df = CSI_Stock.get_all(to_df=True)
   # bi_stock_df = Italy_Stock.get_all()
    #bi_fondi_df = Italy_FONDI.get_all()

    # get all sub exchanges in csi stocks
    csi_sub_exch_df = csi_stock_df[['exchange', 'sub_exchange']]
    csi_sub_exch_df = csi_sub_exch_df.drop_duplicates()
    csi_sub_exch_df = csi_sub_exch_df.dropna()

    bi_sub_exch_df = bi_stock_df[['nome', 'mercato']]
 #   bi_fondi_df = bi_fondi_df[['denominazione', 'mercato']]
 #   bi_fondi_df.columns = ['nome', 'mercato']
    bi_sub_exch_df = bi_sub_exch_df.append(bi_fondi_df[['nome', 'mercato']])
    bi_sub_exch_df['nome'] = 'MIL'
    bi_sub_exch_df.columns = ['exchange', 'sub_exchange']
    bi_sub_exch_df = bi_sub_exch_df.drop_duplicates()

    sub_exch_df = pd.concat([csi_sub_exch_df, bi_sub_exch_df])
    # replace exchange with own id
    exch_id = exch_df.set_index('symbol').to_dict()['exchange_id']
    sub_exch_df['exchange_id'] = sub_exch_df['exchange'].replace(exch_id)
    sub_exch_df.drop('exchange', axis=1,inplace=True)

    # Check if every exchange is present on database
    if not sub_exch_df.loc[~sub_exch_df['exchange_id'].apply(np.isreal)].empty:
        exch_not_listed = sub_exch_df.loc[~sub_exch_df['exchange_id'].apply(np.isreal)]['exchange_id'].to_list()
        logger.error("FINDING EXCHANGES NOT PRESENT IN DATABASE: %s" % (','.join(list(set(exch_not_listed)))))
        sub_exch_df = sub_exch_df.loc[sub_exch_df['exchange_id'].apply(np.isreal)]

    # Find the values that are different between the two DataFrames
    altered_values_df = altered_values(
        existing_df=existing_df, new_df=sub_exch_df)

    # Prepare a new DataFrame with all relevant data for these values
    df = pd.DataFrame()
    df.insert(0, 'subexchange_name', altered_values_df['sub_exchange'])
    df.insert(1, 'exchange_id', altered_values_df['exchange_id'])
    logger.info('Finished processing the Sub Exchange IDs taking %0.2f seconds' % (time.time() - start))
    await database.disconnect()
    return df


