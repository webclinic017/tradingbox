from api.db_api import database, metadata, sqlalchemy
from sqlalchemy.sql import select, and_, delete

from models.utils import check_result, check_value

from datetime import datetime

import pandas as pd


########################################################################################################################

sectors = sqlalchemy.Table(
    'sectors',
    metadata,
    sqlalchemy.Column("sector_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("type_id", sqlalchemy.SmallInteger,  nullable=False), #sector_type
    sqlalchemy.Column("sector", sqlalchemy.String),
    sqlalchemy.Column("industry", sqlalchemy.String),
    sqlalchemy.Column("code", sqlalchemy.String),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

class Sectors:
    tablename = 'sectors'

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(sectors)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    def process_values(cls, row):
        record = {
            'type_id': row['type_id'],
            'sector': row['sector'],
            'industry': row['industry'],
            'code': row['code']
        }
        return record

    @classmethod
    async def find_by_sector_industry_and_type(cls, sector: str, industry: str, sector_type: str, code: str):
        query = select(sectors.c.sector_id, sectors.c.type_id, sectors.c.sector, sectors.c.industry, sectors.c.code) \
            .where(and_(sectors.c.sector == sector, sectors.c.industry == industry, sectors.c.type_id == sector_type,
                        sectors.c.code == code))
        result = await database.fetch_one(query)
        if result:
            result = result[0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, row):
        row_id = await Sectors.find_by_sector_industry_and_type(row['sector'], row['industry'],
                                                                row['type_id'], row['code'])
        if row_id:
            query = sectors.update().where(sectors.c.sector_id == row_id).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = sectors.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


########################################################################################################################

currencies = sqlalchemy.Table(
    'currencies',
    metadata,
    sqlalchemy.Column("currency_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("currency_code", sqlalchemy.String(8)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Currencies:
    tablename = 'currencies'

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(currencies)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    def process_values(cls, row):
        record = {
            'currency_code': row['currency_code'],
        }
        return record

    @classmethod
    async def find_by_code(cls, currency_code: str):
        query = select(currencies.c.currency_id).where(currencies.c.currency_code == currency_code)
        result = await database.fetch_one(query)
        if result:
            result = result[0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, row):
        row_id = await Currencies.find_by_code(row['currency_code'])
        if row_id:
            query = currencies.update().where(currencies.c.currency_id == row_id).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = currencies.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id

########################################################################################################################


tickers = sqlalchemy.Table(
    'tickers',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(20), primary_key=True),
    sqlalchemy.Column("symbol", sqlalchemy.String(12), nullable=False),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("tsid", sqlalchemy.String(50),  nullable=False), #ForeignKey(Symbology.id)
    sqlalchemy.Column("is_active", sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column("exchange_id", sqlalchemy.SmallInteger, nullable=False), # ForeignKey(Exchange.exchange_id)
    sqlalchemy.Column("sub_exchange_id", sqlalchemy.SmallInteger), # ForeignKey(SubExchange.subexchange_id)
    sqlalchemy.Column("asset_type_id", sqlalchemy.SmallInteger, nullable=False), # ForeignKey(Asset_Type.asset_type_id
    sqlalchemy.Column("sector_id", sqlalchemy.SmallInteger), # ForeignKey(Sector.sector_id))
    sqlalchemy.Column("currency_id", sqlalchemy.SmallInteger), #ForeignKey(Currency.currency_id))
    sqlalchemy.Column("market_cap_scale_id", sqlalchemy.SmallInteger), #ForeignKey(Scale.scale_id))
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Tickers:
    tablename = 'tickers'

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(tickers)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_active_symbols(cls, exchange_list=None, to_df=True):
        if exchange_list is None:
            query = select(tickers.c.symbol_id).where(tickers.c.is_active == True)
        else:
            query = select(tickers.c.symbol_id).where(and_(tickers.c.exchange_id.in_(exchange_list),
                                                           tickers.c.is_active == True))
        result = await database.fetch_all(query)

        if to_df:
           result = pd.DataFrame.from_records(result)
           if result.empty:
               result = pd.DataFrame(columns=['symbol_id'])
        else:
            if not result:
                result = False
        return result

    @classmethod
    async def get_id_by_tsid(cls, tsid: int):
        query = select(tickers.c.symbol_id).where(tickers.c.tsid == tsid)
        result = await database.fetch_one(query)
        if result:
            result = result[0]
        else:
            result = False
        return result


    @classmethod
    async def create_or_update(cls, row):  # , to_df=True):
        tsid = row['tsid']
        if tsid is None:
            return
        else:
            row_id = await Tickers.get_id_by_tsid(tsid)
            if row_id:
                query = tickers.update().where(tickers.c.symbol_id == row_id).values(**row)
                row_id = await database.execute(query=query)
            else:
                query = tickers.insert().values(**row)
                row_id = await database.execute(query=query)
            return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'name': row['name'],
            'tsid': row['tsid'],
            'is_active': row['is_active'],
            'exchange_id': check_value(int, row, 'exchange_id') if row['exchange_id'] != 0 else None,
            'sub_exchange_id': check_value(int, row, 'sub_exchange_id') if row['sub_exchange_id'] != 0 else None,
            'asset_type_id': check_value(int, row, 'asset_type_id') if row['asset_type_id'] != 0 else None,
            'sector_id': check_value(int, row, 'sector_id'),
            'currency_id': check_value(int, row, 'currency_id'),
            'market_cap_scale_id': check_value(int, row,
                                               'market_cap_scale_id') if row['market_cap_scale_id'] != 0 else None,
        }
        return record


########################################################################################################################


indices = sqlalchemy.Table(
    'indices',
    metadata,
    sqlalchemy.Column("ticker_id", sqlalchemy.String(20), primary_key=True, nullable=False), # ForeignKey(Ticker.symbol_id)
    sqlalchemy.Column("index_id", sqlalchemy.String(20), primary_key=True, nullable=False),  # ForeignKey(Ticker.symbol_id)
    sqlalchemy.Column("is_active", sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)

class Indices:
    tablename = 'indices'

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(indices)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def delete(cls):
        query = delete(indices)
        result = await database.execute(query)
        return result

    @classmethod
    async def find_item(cls, ticker_id, index_id, to_df=True):
        query = select(indices).where(and_(indices.c.ticker_id == ticker_id, indices.c.index_id == index_id))
        result = await database.fetch_one(query)
        if result:
            if to_df:
                result = pd.Series({'ticker_id': result['ticker_id'],
                                    'index_id': result['index_id'],
                                    'is_active': result['is_active'],
                                    })
            else:
                result = pd.Series()
        else:
            result = False
        return result

    @classmethod
    def process_values(cls, row):
        record = {
            'ticker_id': row['ticker_id'],
            'index_id': row['index_id'],
            'is_active': row['is_active']
        }
        return record

    @classmethod
    async def create_or_update(cls, row):  # , to_df=True):
        row_id = await Indices.find_item(row['ticker_id'], row['index_id'])
        if row_id:
            query = indices.update().where(and_(indices.c.ticker_id == row['ticker_id'],
                                                indices.c.index_id == row['index_id'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = indices.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id

########################################################################################################################
