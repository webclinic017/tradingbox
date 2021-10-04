import pandas as pd
import numpy as np

from settings.config import CSI_INDEX_EXCHANGE, CSI_MUTUAL_EXCHANGE

nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']



######################################################################################################################


def get_stocks_csi(df):
    df = df.loc[(df['is_etn'] == False) & (df['is_etf'] == False) & (~df['exchange'].isin(CSI_INDEX_EXCHANGE)) &
        (~df['exchange'].isin(CSI_MUTUAL_EXCHANGE)) & (~df['exchange'].isin(nothing)) & (~df['symbol'].isin(nothing))]

    df = df[['csi_number','symbol','name','exchange','sub_exchange','is_active']]
    df.columns = ['sid', 'ticker', 'name', 'exchange', 'sub_exchange', 'is_active']
    return df


def get_etf(df):
    df = df.loc[(df['is_etn'] == False) & (df['is_etf'] == True) & (~df['exchange'].isin(CSI_INDEX_EXCHANGE)) &
        (~df['exchange'].isin(CSI_MUTUAL_EXCHANGE)) & (~df['exchange'].isin(nothing)) & (~df['symbol'].isin(nothing))]

    df = df[['csi_number','symbol','name','exchange','sub_exchange','is_active']]
    df.columns = ['sid', 'ticker', 'name', 'exchange', 'sub_exchange', 'is_active']
    return df


def get_etn(df):
    df = df.loc[(df['is_etn'] == True) & (df['is_etf'] == False) & (~df['exchange'].isin(CSI_INDEX_EXCHANGE)) &
        (~df['exchange'].isin(CSI_MUTUAL_EXCHANGE)) & (~df['exchange'].isin(nothing)) & (~df['symbol'].isin(nothing))]

    df = df[['csi_number','symbol','name','exchange','sub_exchange','is_active']]
    df.columns = ['sid', 'ticker', 'name', 'exchange', 'sub_exchange', 'is_active']
    return df


def get_mutual(df):
    df = df.loc[df['exchange'].isin(CSI_MUTUAL_EXCHANGE)]

    df = df[['csi_number','symbol','name','exchange','sub_exchange','is_active']]
    df.columns = ['sid', 'ticker', 'name', 'exchange', 'sub_exchange', 'is_active']
    return df


def get_index(df):
    df = df.loc[df['exchange'].isin(CSI_INDEX_EXCHANGE)]

    df = df[['csi_number', 'symbol', 'name', 'exchange', 'sub_exchange', 'is_active']]
    df.columns = ['sid', 'ticker', 'name', 'exchange', 'sub_exchange', 'is_active']
    return df

######################################################################################################################


def clean_csi_df(input_df, asset_id):
    new_df = input_df.sort_values(by=['ticker', 'exchange', 'is_active'], axis=0,
                                  ascending=[True, True, False])
    new_df = new_df.drop_duplicates(subset=['ticker', 'exchange'])
    new_df['asset_id'] = asset_id
    return new_df


def clean_bi_df(input_df, asset_id):
    new_df = input_df[['codice_isin', 'codice_alfanumerico', 'nome']].copy()
    new_df.columns = ['sid', 'ticker', 'name']
    new_df['exchange'] = 'MIL'
    if 'mercato' in input_df.columns:
        new_df['sub_exchange'] = input_df['mercato']
    else:
        new_df['sub_exchange'] = None
    new_df['is_active'] = 1
    new_df = new_df.sort_values(by=['ticker', 'exchange', 'is_active'], axis=0, ascending=[True, True, False])
    new_df = new_df.drop_duplicates(subset=['ticker', 'exchange'])
    new_df['asset_id'] = asset_id
    return new_df

