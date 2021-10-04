from models.base import Data_Vendors
from models.utils import DECIMAL, INT_DIGITS, nothing

from sqlalchemy import Index
from sqlalchemy import select, func, and_
from sqlalchemy_utils import create_view, create_materialized_view

from datetime import datetime
import pandas as pd

from api.db_api import database, metadata, sqlalchemy

from sqlalchemy_utils.view import CreateView
from sqlalchemy.ext import compiler

@compiler.compiles(CreateView)
def compile_create_materialized_view(element, compiler, **kw):
    return 'CREATE OR REPLACE {}VIEW {} AS {}'.format(
        'MATERIALIZED ' if element.materialized else '',
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
)


##########################################################
daily_prices = sqlalchemy.Table(
    'daily_prices',
    metadata,
    sqlalchemy.Column("data_vendor_id", sqlalchemy.Integer, primary_key=True, nullable=False),
    sqlalchemy.Column("symbol_id", sqlalchemy.String(20), primary_key=True, nullable=False),
    sqlalchemy.Column("date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("open", sqlalchemy.Numeric(20, 6)),
    sqlalchemy.Column("high", sqlalchemy.Numeric(20, 6)),
    sqlalchemy.Column("low", sqlalchemy.Numeric(20, 6)),
    sqlalchemy.Column("close", sqlalchemy.Numeric(20, 6)),
    sqlalchemy.Column("adjclose", sqlalchemy.Numeric(20, 6)),
    sqlalchemy.Column("volume", sqlalchemy.BigInteger),
    sqlalchemy.Column("dividends", sqlalchemy.Numeric(10, 6)),
    sqlalchemy.Column("splits", sqlalchemy.Numeric(10, 6)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
    sqlalchemy.PrimaryKeyConstraint('data_vendor_id', 'symbol_id', 'date', name='daily_prices_pk'),
)

Index("daily_prices_index", daily_prices.c.data_vendor_id, daily_prices.c.symbol_id, daily_prices.c.date.desc())


class Daily_Prices:
    tablename = 'daily_prices'

    @classmethod
    async def is_empty(cls):
        query = select(daily_prices).first()
        result = await database.fetch_one(query)
        return result

    @classmethod
    async def get_last_date_by_symbol_id(cls, symbol_id, vendor_id, to_df=True):
        query_last_prices = select(daily_prices.c.symbol_id, func.max(daily_prices.c.date)).where(and_(
                    daily_prices.c.data_vendor_id == vendor_id, daily_prices.c.symbol_id == symbol_id)
                    ).group_by(daily_prices.c.symbol_id)
        result = await database.fetch_one(query_last_prices)
        if to_df:
            df = pd.DataFrame(result, columns=['symbol_id', 'date'])
            if len(df.index) > 0:
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
            #                df['updated_date'] = pd.to_datetime(df['updated_date'], utc=True)
            return df

        return result

    @classmethod
    async def get_last_price(cls, vendor_id, to_df=True):
        query_last_prices = select(daily_prices.c.symbol_id, func.max(daily_prices.c.date)).where(
                    daily_prices.c.data_vendor_id == vendor_id).group_by(daily_prices.c.symbol_id)
        result = await database.fetch_all(query_last_prices)
        if to_df:
            df = pd.DataFrame.from_records(result, columns=['symbol_id', 'date'])
            if len(df.index) > 0:
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
#                df['updated_date'] = pd.to_datetime(df['updated_date'], utc=True)
            return df

        return result

    @classmethod
    async def get_prices_by_symbol_id(cls, vendor_name, symbol_id, to_df=True):
        vendor_id = await Data_Vendors.get_data_vendor_id(vendor_name)[0]
        query_prices = select(daily_prices).where(and_(daily_prices.c.symbol_id == symbol_id),
                                                  (daily_prices.c.data_vendor_id == vendor_id))
        result = await database.fetch_all(query_prices)
        if to_df:
            df = pd.DataFrame.from_records(result, parse_dates=['date'])
            if len(df.index) > 0:
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df.set_index('date', inplace=True)
            return df
        return result

    @classmethod
    def process_values(cls, row):
        record = {
            'data_vendor_id': row['data_vendor_id'],
            'symbol_id': row['symbol_id'],
            'date': row['date'],
            'open': round_value(row['open'], decimal=2),
            'high': round_value(row['high'], decimal=2),
            'low': round_value(row['low'], decimal=2),
            'close': round_value(row['close'], decimal=2),
            'adjclose': round_value(row['adjclose'], decimal=2),
            'volume': row['volume'],
            'dividends': round_value(row['dividends'], int_digit=4),
            'splits': round_value(row['splits'], int_digit=4)
        }
        return record

    @classmethod
    def round_bar(cls, row):
        row['open'] = round_value(row['open'], decimal=2)
        row['high'] = round_value(row['high'], decimal=2)
        row['low'] = round_value(row['low'], decimal=2)
        row['close'] = round_value(row['close'], decimal=2)
        row['adjclose'] = round_value(row['adjclose'], decimal=2)
        row['dividends'] = round_value(row['dividends'], int_digit=4)
        return row


daily_last_prices = select(daily_prices.c.symbol_id, daily_prices.c.data_vendor_id, func.max(daily_prices.c.date))\
                    .group_by(daily_prices.c.symbol_id, daily_prices.c.data_vendor_id)

# attaches the view to the metadata using the select statement
daily_last_view = create_view('daily_lastprices', daily_last_prices, metadata)

# provides an ORM interface to the view
class Daily_LastPrices:
    __table__ = daily_last_view

    @classmethod
    async def get_by_vendor(cls, vendor_id, to_df=True):
        query = select(daily_last_prices.c).where(daily_last_prices.c.data_vendor_id == vendor_id)
        result = await database.fetch_all(query)
        if to_df:
            df = pd.DataFrame.from_records(result)
            if len(df.index) > 0:
                df.rename(columns={'max_1': 'date'}, inplace=True)
                df = df[['symbol_id', 'date']]
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
            else:
                df = pd.DataFrame(columns=['symbol_id', 'date'])
            return df
        else:
            return result

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(daily_last_prices.c) #.where(daily_last_prices.c.data_vendor_id == vendor_id)
        result = await database.fetch_all(query)
        if to_df:
            df = pd.DataFrame.from_records(result)
            if len(df.index) > 0:
                df.rename(columns={'max_1': 'date'}, inplace=True)
                df = df[['symbol_id', 'date']]
                # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
            else:
                df = pd.DataFrame(columns=['symbol_id', 'date'])
            return df
        else:
            return result

# Index('daily_prices_index', Daily_Prices.data_vendor_id, Daily_Prices.symbol_id, Daily_Prices.date.desc())

def round_value(value, decimal=DECIMAL, int_digit=INT_DIGITS):
    value = round(value, decimal) if value not in nothing else None
    if value:
        if value <= 1 / 10**decimal:
            value = 0
        if value >= 10 ** int_digit:
            value = 10**int_digit - 1
    return value

########################################################################################################################
"""
class Minute_Prices(Base):
    __tablename__ = 'minute_prices'

    data_vendor_id = Column(Integer, primary_key=True, nullable=False)
    symbol_id = Column(String(20), primary_key=True, nullable=False)
    date = Column(DateTime, primary_key=True, nullable=False)
    open = Column(Numeric(20, 6))
    high = Column(Numeric(20, 6))
    low = Column(Numeric(20, 6))
    close = Column(Numeric(20, 6))
    adjclose = Column(Numeric(20, 6))
    volume = Column(BigInteger)
    dividends = Column(Numeric(10, 6))
    splits = Column(Numeric(10, 6))
    dt_created = Column(DateTime, default=datetime.utcnow)
    dt_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    PrimaryKeyConstraint('data_vendor_id', 'symbol_id', 'date', name='min_prices_pk')



    FOREIGN KEY(data_vendor_id)
        REFERENCES data_vendor(data_vendor_id),
    FOREIGN KEY(source, source_id)
        REFERENCES symbology(source, source_id)
        ON UPDATE CASCADE)


    @classmethod
    def get_last_price(cls, table, vendor_id, to_df=True):
        df = None

        query = "SELECT sym.source_id, prices.date,
                                prices.updated_date
                            FROM symbology AS sym,
                            LATERAL (
                                SELECT date, updated_date
                                FROM minute_prices
                                WHERE source_id = sym.source_id
                                AND source = sym.source
                                AND data_vendor_id IN (%s)
                                ORDER BY source_id, date DESC NULLS LAST
                                LIMIT 1) AS prices" ',(vendor_id,))'

        with session_local() as session:
            with session.begin():
                result = session.execute(query).all()

        if to_df:
            df = pd.DataFrame(result, columns=['tsid', 'date', 'updated_date'])
            df.set_index(['tsid'], inplace=True)

            if len(df.index) > 0:
            # Convert the ISO dates to datetime objects
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df['updated_date'] = pd.to_datetime(df['updated_date'], utc=True)
                # df.to_csv('query_last_price.csv')
            return df

        return result

Index('idx_mp_identifiers', Minute_Prices.source, Minute_Prices.source_id, Minute_Prices.data_vendor_id,
      Minute_Prices.date.desc().nullslast(), Minute_Prices.updated_date)

"""

########################################################################################################################
