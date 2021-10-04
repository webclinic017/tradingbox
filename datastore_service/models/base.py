from datetime import datetime
import pandas as pd

from api.db_api import database, metadata, sqlalchemy
from sqlalchemy.sql import select, and_

from models.utils import check_result

########################################################################################################################

data_vendors = sqlalchemy.Table(
    'data_vendors',
    metadata,
    sqlalchemy.Column("data_vendor_id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String, unique=True),
    sqlalchemy.Column("url", sqlalchemy.String),
    sqlalchemy.Column("support_email", sqlalchemy.String),
    sqlalchemy.Column("api", sqlalchemy.String),
    sqlalchemy.Column("source", sqlalchemy.String),
    sqlalchemy.Column("consensus_weight", sqlalchemy.SmallInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Data_Vendors:
    @classmethod
    async def get_data_vendor_id(cls, vendor_name):
        vendor_name = "%" + vendor_name + "%"
        """SELECT data_vendor_id FROM data_vendor WHERE name LIKE '%s'"""
        query = select(data_vendors.c.data_vendor_id).where(data_vendors.c.name.like(vendor_name))
        result = await database.fetch_all(query=query)
        if result:  # A vendor was found
            if len(result) == 1:
                # Only one vendor id returned, so only return that value
                result = result[0]['data_vendor_id']  # result[0][0]
            else:
                # Multiple vendor ids were returned; return a list of them
                df = pd.DataFrame(result, columns=['data_vendor_id'])
                result = df['data_vendor_id'].values.tolist()
        else:
            result = False
        #     logger.info('Not able to determine the data_vendor_id for %s' % vendor_name)
        return result

    @classmethod
    async def get_vendor_by_name(cls, vendor_name, to_df=True):
        vendor_name = "%" + vendor_name + "%"
        """SELECT data_vendor_id FROM data_vendor WHERE name LIKE '%s'"""
        query = select(data_vendors).where(data_vendors.c.name.like(vendor_name))
        result = await database.fetch_all(query=query)
        if to_df:
            result = pd.DataFrame.from_records(result)
        return result

    @classmethod
    async def create_or_update(cls, data_vendor_in):
        #    data_vendor = Data_Vendors.process_values(data_vendor_in)
        row_id = await Data_Vendors.get_data_vendor_id(data_vendor_in['name'])
        if row_id:
            query = data_vendors.update().where(data_vendors.c.data_vendor_id == row_id).values(**data_vendor_in)
            row_id = await database.execute(query=query)
        else:
            query = data_vendors.insert().values(**data_vendor_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'data_vendor_id': row['data_vendor_id'],
            'name': row['name'],
            'url': row['url'],
            'support_email': row['support_email'],
            'api': row['api'],
            'source': row['source'],
            'consensus_weight': row['consensus_weight']
        }
        #        result = self(**record)
        return record


########################################################################################################################

exchanges = sqlalchemy.Table(
    'exchanges',
    metadata,
    sqlalchemy.Column("exchange_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("symbol", sqlalchemy.String, unique=True, nullable=False),
    sqlalchemy.Column("goog_symbol", sqlalchemy.String),
    sqlalchemy.Column("yahoo_symbol", sqlalchemy.String),
    sqlalchemy.Column("csi_symbol", sqlalchemy.String),
    sqlalchemy.Column("tsid_symbol", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("country", sqlalchemy.String),
    sqlalchemy.Column("city", sqlalchemy.String),
    sqlalchemy.Column("currency", sqlalchemy.String),
    sqlalchemy.Column("time_zone", sqlalchemy.String),
    sqlalchemy.Column("utc_offset", sqlalchemy.Float),
    sqlalchemy.Column("open", sqlalchemy.String),
    sqlalchemy.Column("close", sqlalchemy.String),
    sqlalchemy.Column("lunch", sqlalchemy.String),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Exchanges:
    @classmethod
    async def get_all(cls, to_df=True):
        # SELECT symbol, name, goog_symbol, yahoo_symbol, csi_symbol, tsid_symbol FROM exchanges
        query = select(exchanges)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_exchange_id(cls, exchange_symbol):
        query = select(exchanges.c.exchange_id).where(exchanges.c.symbol == exchange_symbol)
        result = await database.fetch_all(query=query)
        if result:
            result = result[0]['exchange_id']  # result[0][0]
        else:
            result = False
        return result

    @classmethod
    async def get_by_country(cls, country, to_df=True):
        query = select(exchanges.c.exchange_id).where(exchanges.c.country == country)
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result)
        return result

    @classmethod
    async def get_yahoo_exchanges(cls, to_df=True):
        query = select(exchanges.c.exchange_id, exchanges.c.symbol, exchanges.c.yahoo_symbol,
                       exchanges.c.tsid_symbol).where(
            and_(exchanges.c.yahoo_symbol != 'NaN', exchanges.c.yahoo_symbol != None)).order_by(
            exchanges.c.tsid_symbol.desc().nullslast()).distinct(exchanges.c.tsid_symbol)
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['exchange_id', 'symbol', 'yahoo_symbol', 'tsid_symbol'])
        return result

    @classmethod
    async def create_or_update(cls, exchange_in):
        row_id = await Exchanges.get_exchange_id(exchange_in['symbol'])
        if row_id:
            query = exchanges.update().where(exchanges.c.exchange_id == row_id).values(**exchange_in)
            row_id = await database.execute(query=query)
        else:
            query = exchanges.insert().values(**exchange_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'exchange_id': row[0],
            'symbol': row[1],
            'goog_symbol': row[2],
            'yahoo_symbol': row[3],
            'csi_symbol': row[4],
            'tsid_symbol': row[5],
            'name': row[6],
            'country': row[7],
            'city': row[8],
            'currency': row[9],
            'time_zone': row[10],
            'utc_offset': row[11],
            'open': row[12],
            'close': row[13],
            'lunch': row[14]
        }
        return record


########################################################################################################################

asset_types = sqlalchemy.Table(
    'asset_types',
    metadata,
    sqlalchemy.Column("asset_type_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("asset_type_name", sqlalchemy.String(32), nullable=False),
    sqlalchemy.Column("asset_type_code", sqlalchemy.String(32)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Asset_Types:
    @classmethod
    async def get_all(cls, to_df=True):
        query = select(asset_types)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_asset_type_id(cls, asset_type_name):
        query = select(asset_types.c.asset_type_id).where(asset_types.c.asset_type_name == asset_type_name)
        result = await database.fetch_one(query=query)
        if result:
            result = result[0]  # ['asset_type_id']  # result[0][0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, asset_type_in):
        row_id = await Asset_Types.get_asset_type_id(asset_type_in['asset_type_name'])
        if row_id:
            query = asset_types.update().where(asset_types.c.asset_type_id == row_id).values(**asset_type_in)
            row_id = await database.execute(query=query)
        else:
            query = asset_types.insert().values(**asset_type_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'asset_type_id': row[0],
            'asset_type_name': row[1],
            'asset_type_code': row[2]
        }
        return record


########################################################################################################################

market_cap_scale = sqlalchemy.Table(
    'market_cap_scale',
    metadata,
    sqlalchemy.Column("scale_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("scale_name", sqlalchemy.String(20)),
    sqlalchemy.Column("min_value", sqlalchemy.BigInteger),
    sqlalchemy.Column("max_value", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Scale:

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(market_cap_scale)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_scale_id(cls, scale_name):
        query = select(market_cap_scale.c.scale_id).where(market_cap_scale.c.scale_name == scale_name)
        result = await database.fetch_one(query=query)
        if result:
            result = result[0]  # ['scale_id']  # result[0][0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, scale_in):
        row_id = await Scale.get_scale_id(scale_in['scale_name'])
        if row_id:
            query = market_cap_scale.update().where(market_cap_scale.c.scale_id == row_id).values(**scale_in)
            row_id = await database.execute(query=query)
        else:
            query = market_cap_scale.insert().values(**scale_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'scale_id': row['scale_id'],
            'scale_name': row['scale_name'],
            'min_value': row['min_value'],
            'max_value': row['max_value']
        }
        return record


########################################################################################################################

sector_types = sqlalchemy.Table(
    'sector_types',
    metadata,
    sqlalchemy.Column("sector_type_id", sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column("sector_type_name", sqlalchemy.String(32)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Sector_Types:

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(sector_types)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_sector_type_id(cls, sector_type_name):
        query = select(sector_types.c.sector_type_id).where(sector_types.c.sector_type_name == sector_type_name)
        result = await database.fetch_one(query=query)
        if result:
            result = result[0]  # ['sector_type_id']  # result[0][0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, sector_type_in):
        row_id = await Sector_Types.get_sector_type_id(sector_type_in['sector_type_name'])
        if row_id:
            query = sector_types.update().where(sector_types.c.sector_type_id == row_id).values(**sector_type_in)
            row_id = await database.execute(query=query)
        else:
            query = sector_types.insert().values(**sector_type_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'sector_type_id': row[0],
            'sector_type_name': row[1]
        }
        return record


########################################################################################################################


sub_exchanges = sqlalchemy.Table(
    'sub_exchanges',
    metadata,
    sqlalchemy.Column("subexchange_id", sqlalchemy.SmallInteger, primary_key=True, autoincrement=True),
    sqlalchemy.Column("subexchange_name", sqlalchemy.String(100), nullable=False),
    sqlalchemy.Column("exchange_id", sqlalchemy.SmallInteger, nullable=False),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class SubExchanges:
    tablename = 'sub_exchanges'

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(sub_exchanges)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def get_subexchanges(cls, to_df=False):
        query = select(sub_exchanges.c.subexchange_id, sub_exchanges.c.subexchange_name, sub_exchanges.c.exchange_id)
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result)
        return result

    @classmethod
    async def get_from_exchange_id(cls, exchange_id, to_df: False):
        query = select(sub_exchanges.c.subexchange_id, sub_exchanges.c.subexchange_name).where(
            sub_exchanges.c.exchange_id == exchange_id)
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['subexchange_id', 'subexchange_name'])
        return result

    @classmethod
    async def get_sub_exchange_id(cls, sub_exchange_name, exchange_id):
        query = select(sub_exchanges.c.subexchange_id).where(and_(sub_exchanges.c.subexchange_name == sub_exchange_name,
                                                                  sub_exchanges.c.exchange_id == exchange_id))
        result = await database.fetch_one(query=query)
        if result:
            result = result[0]  # ['subexchange_id']  # result[0][0]
        else:
            result = False
        return result

    @classmethod
    async def create_or_update(cls, sub_exchange_in):
        row_id = await SubExchanges.get_sub_exchange_id(sub_exchange_in['subexchange_name'],
                                                        sub_exchange_in['exchange_id'])
        if row_id:
            query = sub_exchanges.update().where(sub_exchanges.c.subexchange_id == row_id).values(**sub_exchange_in)
            row_id = await database.execute(query=query)
        else:
            query = sub_exchanges.insert().values(**sub_exchange_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'subexchange_name': row[0],
            'exchange_id': int(row[1])
        }
        return record


#######################################################################################################################
#                           SYMBOLOGY

symbology = sqlalchemy.Table(
    'symbology',
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(50), primary_key=True, nullable=False),  # concat of "symbol_id_source"
    sqlalchemy.Column("symbol_id", sqlalchemy.String(20), nullable=False),
    sqlalchemy.Column("source", sqlalchemy.String(20), nullable=False),
    sqlalchemy.Column("source_id", sqlalchemy.String(20), nullable=False),
    sqlalchemy.Column("is_active", sqlalchemy.SmallInteger),
    sqlalchemy.Column("type", sqlalchemy.String(20)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Symbology:
    """
    The symbology table is used as a translator between the tsid symbol and other symbol structures (Quandl codes, Yahoo
    Finance codes, etc.) (Note 2). This structure enables future symbol structures to be seamlessly added to the table
    to allow for external database communication (RIC, Bloomberg, etc.).

    Not only does this translation ability allows you convert one source's symbol to another, but it allows you to query
    any source's symbols based on characteristics stored in other tables (exchange, sector, industry, etc.).

    By default, the symbology table links the tsid symbol to these data sources (Note 3):
    """

    tablename = 'symbology'

    @classmethod
    async def get_by_id(cls, tsid: int, to_df=True):
        query = select(symbology.c.symbol_id, symbology.c.source, symbology.c.source_id, symbology.c.is_active,
                       symbology.c.type).where(symbology.c.id == tsid)
        result = await database.fetch_one(query)
        if result and to_df:
            result = pd.Series({'symbol_id': result['symbol_id'],
                                'source': result['source'],
                                'source_id': result['source_id'],
                                'is_active': result['is_active'],
                                'type': result['type']})
        elif to_df:
            result = pd.Series()
        elif not result:
            result = False
        return result

    @classmethod
    async def get_by_symbol_id(cls, symbol_id: int, source: str, to_df=True):
        query = select(symbology.c.id, symbology.c.symbol_id, symbology.c.source, symbology.c.source_id,
                       symbology.c.is_active, symbology.c.type).where(symbology.c.symbol_id == symbol_id,
                                                                      symbology.c.source == source)
        result = await database.fetch_one(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['id', 'symbol_id', 'source',
                                                                'source_id', 'is_active', 'type'])
        return result

    @classmethod
    async def get_by_source(cls, source, stype, to_df=True):
        # SELECT symbol_id, source_id  FROM symbology WHERE source = % s , (source,))
        query = select(symbology.c.id, symbology.c.symbol_id, symbology.c.source_id, symbology.c.is_active) \
            .where(and_(symbology.c.source == source, symbology.c.type == stype))
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['id', 'symbol_id', 'source_id', 'is_active'])
        return result

    @classmethod
    async def get_by_source_id(cls, source, source_id, to_df=True):
        # SELECT symbol_id, source_id  FROM symbology WHERE source = % s , (source,)) 
        query = select(symbology.c.id, symbology.c.symbol_id, symbology.c.source_id, symbology.c.is_active) \
            .where(and_(symbology.c.source == source, symbology.c.source_id == source_id))
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['id', 'symbol_id', 'source_id', 'is_active'])
        return result

    @classmethod
    async def get_tsid_based_on_exchanges(cls, exchanges_list, to_df=True):
        # SELECT DISTINCT ON (source_id) source_id FROM symbology WHERE source='tsid' %s ORDER BY source_id %
        # (exchanges_query,)
        query = select(symbology.c.source_id).where(symbology.c.source == 'tsid') \
            .order_by(symbology.c.source_id).distinct(symbology.c.source_id)
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['source_id'])
            # Get subset based on exchanges
            result['exchange'] = result.apply(lambda x: x['source_id'].split('.')[1], axis=1)
            result = result.loc[result['exchange'].isin(exchanges_list)]
            result.drop('exchange', axis=1, inplace=True)
        return result

    @classmethod
    async def get_all_by_symbol_id(cls, symbol_ids, vendor, to_df=True):
        query = select(symbology.c.source_id, symbology.c.symbol_id).where(and_(symbology.c.source == vendor,
                                                                                symbology.c.symbol_id.in_(symbol_ids)))
        result = await database.fetch_all(query)
        if to_df:
            result = pd.DataFrame.from_records(result, columns=['source_id', 'symbol_id'])
        return result

    @classmethod
    async def create_or_update(cls, symbology_in):
        row = await Symbology.get_by_id(symbology_in['id'])
        if not row.empty:
            query = symbology.update().where(symbology.c.id == symbology_in['id']).values(**symbology_in)
            row_id = await database.execute(query=query)
        else:
            query = symbology.insert().values(**symbology_in)
            row_id = await database.execute(query=query)
        return row_id

    @classmethod
    def process_values(cls, row):
        record = {
            'id': row['id'],
            'symbol_id': row['symbol_id'],
            'source': row['source'],
            'source_id': row['source_id'],
            'is_active': int(row['is_active']),
            'type': row['type']
        }
        return record

########################################################################################################################
