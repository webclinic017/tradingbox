
from sqlalchemy.sql import select, and_
from api.db_api import database, metadata, sqlalchemy

from models.utils import check_result, check_value

from datetime import datetime


#######################################################################################################################

summary = sqlalchemy.Table(
    'summary',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("address1", sqlalchemy.String),
    sqlalchemy.Column("city", sqlalchemy.String),
    sqlalchemy.Column("zip", sqlalchemy.String(10)),
    sqlalchemy.Column("country", sqlalchemy.String),
    sqlalchemy.Column("phone", sqlalchemy.String(25)),
    sqlalchemy.Column("state", sqlalchemy.String(5)),  # fax", sqlalchemy.String(15))
    sqlalchemy.Column("website", sqlalchemy.String),
    sqlalchemy.Column("industry", sqlalchemy.String),
    sqlalchemy.Column("sector", sqlalchemy.String),
    sqlalchemy.Column("long_business_summary", sqlalchemy.String(1000)),
    sqlalchemy.Column("fulltime_employees", sqlalchemy.Integer),
    sqlalchemy.Column("price_hint", sqlalchemy.SmallInteger),
    sqlalchemy.Column("previous_close", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("open", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("day_low", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("day_high", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("regular_market_previous_close", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("regular_market_open", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("regular_market_day_low", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("regular_market_day_high", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("dividend_rate", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("dividend_yield", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("ex_dividend_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("payout_ratio", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("five_year_avg_dividend_yield", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("beta", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("trailing_pe", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("forward_pe", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("volume", sqlalchemy.BigInteger),
    sqlalchemy.Column("regular_market_volume", sqlalchemy.BigInteger),
    sqlalchemy.Column("average_volume", sqlalchemy.BigInteger),
    sqlalchemy.Column("average_volume_10days", sqlalchemy.BigInteger),
    sqlalchemy.Column("average_daily_volume_10day", sqlalchemy.BigInteger),
    sqlalchemy.Column("bid", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("ask", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("bid_size", sqlalchemy.Integer),
    sqlalchemy.Column("ask_size", sqlalchemy.Integer),
    sqlalchemy.Column("market_cap", sqlalchemy.BigInteger),
    sqlalchemy.Column("fifty_two_week_low", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("fifty_two_week_high", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("price_to_sales_trailing_12months", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("fifty_day_average", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("two_hundred_day_average", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("trailing_annual_dividend_rate", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("trailing_annual_dividend_yield", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("currency", sqlalchemy.String(6)),
    sqlalchemy.Column("from_currency", sqlalchemy.String(6)),
    sqlalchemy.Column("to_currency", sqlalchemy.String(6)),
    sqlalchemy.Column("last_market", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("algorithm", sqlalchemy.String),
    sqlalchemy.Column("tradeable", sqlalchemy.SmallInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Summary:
    tablename = 'summary'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'address1': check_value(None, row, 'address1'),
            'city': check_value(None, row, 'city'),
            'zip': check_value(None, row, 'zip'),
            'country': check_value(None, row, 'country'),
            'phone': check_value(None, row, 'phone'),
            'state': check_value(None, row, 'state'),  # 'fax': check_value(None, row,'fax'),
            'website': check_value(None, row, 'website'),
            'industry': check_value(None, row, 'industry'),
            'sector': check_value(None, row, 'sector'),
            'long_business_summary': check_value(None, row, 'longBusinessSummary', trunc=999),
            'fulltime_employees': check_value(int, row, 'fullTimeEmployees'),
            'price_hint': check_value(int, row, 'priceHint'),
            'previous_close': check_value(float, row, 'previousClose'),
            'open': check_value(float, row, 'open'),
            'day_low': check_value(float, row, 'dayLow'),
            'day_high': check_value(float, row, 'dayHigh'),
            'regular_market_previous_close': check_value(float, row, 'regularMarketPreviousClose'),
            'regular_market_open': check_value(float, row, 'regularMarketOpen'),
            'regular_market_day_low': check_value(float, row, 'regularMarketDayLow'),
            'regular_market_day_high': check_value(float, row, 'regularMarketDayHigh'),
            'dividend_rate': check_value(float, row, 'dividendRate'),
            'dividend_yield': check_value(float, row, 'dividendYield'),
            'ex_dividend_date': check_value('Datetime', row, 'exDividendDate'),
            'payout_ratio': check_value(float, row, 'payoutRatio'),
            'five_year_avg_dividend_yield': check_value(float, row, 'fiveYearAvgDividendYield'),
            'beta': check_value(float, row, 'beta'),
            'trailing_pe': check_value(float, row, 'trailingPE'),
            'forward_pe': check_value(float, row, 'forwardPE'),
            'volume': check_value(int, row, 'volume'),
            'regular_market_volume': check_value(int, row, 'regularMarketVolume'),
            'average_volume': check_value(int, row, 'averageVolume'),
            'average_volume_10days': check_value(int, row, 'averageVolume10days'),
            'average_daily_volume_10day': check_value(int, row, 'averageDailyVolume10Day'),
            'bid': check_value(float, row, 'bid'),
            'ask': check_value(float, row, 'ask'),
            'bid_size': check_value(float, row, 'bidSize'),
            'ask_size': check_value(float, row, 'askSize'),
            'market_cap': check_value(float, row, 'marketCap'),
            'fifty_two_week_low': check_value(float, row, 'fiftyTwoWeekLow'),
            'fifty_two_week_high': check_value(float, row, 'fiftyTwoWeekHigh'),
            'price_to_sales_trailing_12months': check_value(float, row, 'priceToSalesTrailing12Months'),
            'fifty_day_average': check_value(float, row, 'fiftyDayAverage'),
            'two_hundred_day_average': check_value(float, row, 'twoHundredDayAverage'),
            'trailing_annual_dividend_rate': check_value(float, row, 'trailingAnnualDividendRate'),
            'trailing_annual_dividend_yield': check_value(float, row, 'trailingAnnualDividendYield'),
            'currency': check_value(None, row, 'currency'),
            'from_currency': check_value(None, row, 'fromCurrency'),
            'to_currency': check_value(None, row, 'toCurrency'),
            'last_market': check_value(float, row, 'lastMarket'),
            'algorithm': check_value(None, row, 'algorithm'),
            'tradeable': check_value(int, row, 'tradeable')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(summary)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(summary.c.symbol_id).where(summary.c.symbol_id == row['symbol_id'])
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = summary.update().where(summary.c.symbol_id == row['symbol_id']).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = summary.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


######################################################################################################################


fundamentals = sqlalchemy.Table(  # Yahoo_defaultKeyStatistics(Base):
    'fundamentals',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("price_hint", sqlalchemy.SmallInteger),
    sqlalchemy.Column("enterprise_value", sqlalchemy.BigInteger),
    sqlalchemy.Column("forward_pe", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("profit_margins", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("float_shares", sqlalchemy.BigInteger),
    sqlalchemy.Column("shares_outstanding", sqlalchemy.BigInteger),
    sqlalchemy.Column("shares_short", sqlalchemy.BigInteger),
    sqlalchemy.Column("shares_short_prior_month", sqlalchemy.BigInteger),
    sqlalchemy.Column("shares_short_previous_month_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("date_short_interest", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("shares_percent_shares_out", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("held_percent_insiders", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("held_percent_institutions", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("short_ratio", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("short_percent_float", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("beta", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("category", sqlalchemy.String),
    sqlalchemy.Column("book_value", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("price_to_book", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("fund_family", sqlalchemy.String),
    sqlalchemy.Column("legal_type", sqlalchemy.String),
    sqlalchemy.Column("last_fiscal_year_end", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("next_fiscal_year_end", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("most_recent_quarter", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("earnings_quarterly_growth", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("net_income_to_common", sqlalchemy.BigInteger),
    sqlalchemy.Column("trailing_eps", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("forward_eps", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("peg_ratio", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("last_split_factor", sqlalchemy.String),
    sqlalchemy.Column("last_split_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("enterprise_to_revenue", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("enterprise_to_ebitda", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("_52_week_change", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("sand_p52_week_change", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("last_dividend_value", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("last_dividend_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Fundamentals:
    tablename = 'fundamentals'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'price_hint': check_value(int, row, 'priceHint'),
            'enterprise_value': check_value(int, row, 'enterpriseValue'),
            'forward_pe': check_value(float, row, 'forwardPE'),
            'profit_margins': check_value(float, row, 'profitMargins'),
            'float_shares': check_value(int, row, 'floatShares'),
            'shares_outstanding': check_value(int, row, 'sharesOutstanding'),
            'shares_short': check_value(int, row, 'sharesShort'),
            'shares_short_prior_month': check_value(int, row, 'sharesShortPriorMonth'),
            'shares_short_previous_month_date': check_value('Datetime', row, 'sharesShortPreviousMonthDate'),
            'date_short_interest': check_value('Datetime', row, 'dateShortInterest'),
            'shares_percent_shares_out': check_value(float, row, 'sharesPercentSharesOut'),
            'held_percent_insiders': check_value(float, row, 'heldPercentInsiders'),
            'held_percent_institutions': check_value(float, row, 'heldPercentInstitutions'),
            'short_ratio': check_value(float, row, 'shortRatio'),
            'short_percent_float': check_value(float, row, 'shortPercentOfFloat'),
            'beta': check_value(float, row, 'beta'),
            'category': check_value(None, row, 'category'),
            'book_value': check_value(float, row, 'bookValue'),
            'price_to_book': check_value(float, row, 'priceToBook'),
            'fund_family': check_value(None, row, 'fundFamily'),
            'legal_type': check_value(None, row, 'legalType'),
            'last_fiscal_year_end': check_value('Datetime', row, 'lastFiscalYearEnd'),
            'next_fiscal_year_end': check_value('Datetime', row, 'nextFiscalYearEnd'),
            'most_recent_quarter': check_value('Datetime', row, 'mostRecentQuarter'),
            'earnings_quarterly_growth': check_value(float, row, 'earningsQuarterlyGrowth'),
            'net_income_to_common': check_value(float, row, 'netIncomeToCommon'),
            'trailing_eps': check_value(float, row, 'trailingEps'),
            'forward_eps': check_value(float, row, 'forwardEps'),
            'peg_ratio': check_value(float, row, 'pegRatio'),
            'last_split_factor': check_value(None, row, 'lastSplitFactor'),
            'last_split_date': check_value('Datetime', row, 'lastSplitDate'),
            'enterprise_to_revenue': check_value(float, row, 'enterpriseToRevenue'),
            'enterprise_to_ebitda': check_value(float, row, 'enterpriseToEbitda'),
            '_52_week_change': check_value(float, row, '52WeekChange'),
            'sand_p52_week_change': check_value(float, row, 'SandP52WeekChange'),
            'last_dividend_value': check_value(float, row, 'lastDividendValue'),
            'last_dividend_date': check_value('Datetime', row, 'lastDividendDate')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(fundamentals)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(fundamentals.c.symbol_id).where(fundamentals.c.symbol_id == row['symbol_id'])
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = fundamentals.update().where(fundamentals.c.symbol_id == row['symbol_id']).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = fundamentals.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


#######################################################################################################################


financials = sqlalchemy.Table(  # Yahoo_financialData(Base):
    'financials',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("current_price", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("target_high_price", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("target_low_price", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("target_mean_price", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("target_median_price", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("recommendation_mean", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("recommendation_key", sqlalchemy.String),
    sqlalchemy.Column("number_of_analyst_opinions", sqlalchemy.SmallInteger),
    sqlalchemy.Column("total_cash", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cash_per_share", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("ebitda", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_debt", sqlalchemy.BigInteger),
    sqlalchemy.Column("quick_ratio", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("current_ratio", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("total_revenue", sqlalchemy.BigInteger),
    sqlalchemy.Column("debt_to_equity", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("revenue_per_share", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("return_on_assets", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("return_on_equity", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("gross_profits", sqlalchemy.BigInteger),
    sqlalchemy.Column("free_cashflow", sqlalchemy.BigInteger),
    sqlalchemy.Column("operating_cashflow", sqlalchemy.BigInteger),
    sqlalchemy.Column("earnings_growth", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("revenue_growth", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("gross_margins", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("ebitda_margins", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("operating_margins", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("profit_margins", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("financial_currency", sqlalchemy.String(6)),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Financials:  # Yahoo_financialData(Base):
    tablename = 'financials'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'current_price': check_value(float, row, 'currentPrice'),
            'target_high_price': check_value(float, row, 'targetHighPrice'),
            'target_low_price': check_value(float, row, 'targetLowPrice'),
            'target_mean_price': check_value(float, row, 'targetMeanPrice'),
            'target_median_price': check_value(float, row, 'targetMedianPrice'),
            'recommendation_mean': check_value(float, row, 'recommendationMean'),
            'recommendation_key': check_value(None, row, 'recommendationKey'),
            'number_of_analyst_opinions': check_value(int, row, 'numberOfAnalystOpinions'),
            'total_cash': check_value(int, row, 'totalCash'),
            'total_cash_per_share': check_value(float, row, 'totalCashPerShare'),
            'ebitda': check_value(int, row, 'ebitda'),
            'total_debt': check_value(int, row, 'totalDebt'),
            'quick_ratio': check_value(float, row, 'quickRatio'),
            'current_ratio': check_value(float, row, 'currentRatio'),
            'total_revenue': check_value(float, row, 'totalRevenue'),
            'debt_to_equity': check_value(float, row, 'debtToEquity'),
            'revenue_per_share': check_value(float, row, 'revenuePerShare'),
            'return_on_assets': check_value(float, row, 'returnOnAssets'),
            'return_on_equity': check_value(float, row, 'returnOnEquity'),
            'gross_profits': check_value(int, row, 'grossProfits'),
            'free_cashflow': check_value(int, row, 'freeCashflow'),
            'operating_cashflow': check_value(int, row, 'operatingCashflow'),
            'earnings_growth': check_value(float, row, 'earningsGrowth'),
            'revenue_growth': check_value(float, row, 'revenueGrowth'),
            'gross_margins': check_value(float, row, 'grossMargins'),
            'ebitda_margins': check_value(float, row, 'ebitdaMargins'),
            'operating_margins': check_value(float, row, 'operatingMargins'),
            'profit_margins': check_value(float, row, 'profitMargins'),
            'financial_currency': check_value(None, row, 'financialCurrency')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(financials)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(financials.c.symbol_id).where(financials.c.symbol_id == row['symbol_id'])
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = financials.update().where(financials.c.symbol_id == row['symbol_id']).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = financials.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


#######################################################################################################################


balance_sheet_annual = sqlalchemy.Table(
    'balancesheet_annual',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("cash", sqlalchemy.BigInteger),
    sqlalchemy.Column("short_term_investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_receivables", sqlalchemy.BigInteger),
    sqlalchemy.Column("inventory", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_current_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_current_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("long_term_investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("property_plant_equipment", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("accounts_payable", sqlalchemy.BigInteger),
    sqlalchemy.Column("short_long_term_debt", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_current_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("long_term_debt", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_current_liabilities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("common_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("retained_earnings", sqlalchemy.BigInteger),
    sqlalchemy.Column("treasury_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_stockholder_equity", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_stockholder_equity", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_tangible_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class BalanceSheetAnnual:  # Yahoo_balanceSheetHistory(Base):
    tablename = 'balancesheet_annual'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'cash': check_value(int, row, 'cash'),
            'short_term_investments': check_value(int, row, 'shortTermInvestments'),
            'net_receivables': check_value(int, row, 'netReceivables'),
            'inventory': check_value(int, row, 'inventory'),
            'other_current_assets': check_value(int, row, 'otherCurrentAssets'),
            'total_current_assets': check_value(int, row, 'totalCurrentAssets'),
            'long_term_investments': check_value(int, row, 'longTermInvestments'),
            'property_plant_equipment': check_value(int, row, 'propertyPlantEquipment'),
            'other_assets': check_value(int, row, 'otherAssets'),
            'total_assets': check_value(int, row, 'totalAssets'),
            'accounts_payable': check_value(int, row, 'accountsPayable'),
            'short_long_term_debt': check_value(int, row, 'shortLongTermDebt'),
            'other_current_liab': check_value(int, row, 'otherCurrentLiab'),
            'long_term_debt': check_value(int, row, 'longTermDebt'),
            'other_liab': check_value(int, row, 'otherLiab'),
            'total_current_liabilities': check_value(int, row, 'totalCurrentLiabilities'),
            'total_liab': check_value(int, row, 'totalLiab'),
            'common_stock': check_value(int, row, 'commonStock'),
            'retained_earnings': check_value(int, row, 'retainedEarnings'),
            'treasury_stock': check_value(int, row, 'treasuryStock'),
            'other_stockholder_equity': check_value(int, row, 'otherStockholderEquity'),
            'total_stockholder_equity': check_value(int, row, 'totalStockholderEquity'),
            'net_tangible_assets': check_value(int, row, 'netTangibleAssets')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(balance_sheet_annual)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(balance_sheet_annual.c.symbol_id)\
                    .where(and_(balance_sheet_annual.c.symbol_id == row['symbol_id'],
                                balance_sheet_annual.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = balance_sheet_annual.update()\
                    .where(and_(balance_sheet_annual.c.symbol_id == row['symbol_id'],
                                balance_sheet_annual.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = balance_sheet_annual.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


balance_sheet_quarterly = sqlalchemy.Table(  # Yahoo_balanceSheetHistoryQuarterly(Base):
    'balancesheet_quarterly',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("cash", sqlalchemy.BigInteger),
    sqlalchemy.Column("short_term_investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_receivables", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_current_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_current_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("long_term_investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("property_plant_equipment", sqlalchemy.BigInteger),
    sqlalchemy.Column("intangible_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("deferred_long_term_asset_charges", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("accounts_payable", sqlalchemy.BigInteger),
    sqlalchemy.Column("short_long_term_debt", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_current_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("long_term_debt", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_current_liabilities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_liab", sqlalchemy.BigInteger),
    sqlalchemy.Column("common_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("retained_earnings", sqlalchemy.BigInteger),
    sqlalchemy.Column("treasury_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_stockholder_equity", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_stockholder_equity", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_tangible_assets", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class BalanceSheetQuarterly:  # Yahoo_balanceSheetHistoryQuarterly(Base):
    tablename = 'balancesheet_quarterly'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'cash': check_value(int, row, 'cash'),
            'short_term_investments': check_value(int, row, 'shortTermInvestments'),
            'net_receivables': check_value(int, row, 'netReceivables'),
            'other_current_assets': check_value(int, row, 'otherCurrentAssets'),
            'total_current_assets': check_value(int, row, 'totalCurrentAssets'),
            'long_term_investments': check_value(int, row, 'longTermInvestments'),
            'property_plant_equipment': check_value(int, row, 'propertyPlantEquipment'),
            'intangible_assets': check_value(int, row, 'intangibleAssets'),
            'other_assets': check_value(int, row, 'otherAssets'),
            'deferred_long_term_asset_charges': check_value(int, row, 'deferredLongTermAssetCharges'),
            'total_assets': check_value(int, row, 'totalAssets'),
            'accounts_payable': check_value(int, row, 'accountsPayable'),
            'short_long_term_debt': check_value(int, row, 'shortLongTermDebt'),
            'other_current_liab': check_value(int, row, 'otherCurrentLiab'),
            'long_term_debt': check_value(int, row, 'longTermDebt'),
            'other_liab': check_value(int, row, 'otherLiab'),
            'total_current_liabilities': check_value(int, row, 'totalCurrentLiabilities'),
            'total_liab': check_value(int, row, 'totalLiab'),
            'common_stock': check_value(int, row, 'commonStock'),
            'retained_earnings': check_value(int, row, 'retainedEarnings'),
            'treasury_stock': check_value(int, row, 'treasuryStock'),
            'other_stockholder_equity': check_value(int, row, 'otherStockholderEquity'),
            'total_stockholder_equity': check_value(int, row, 'totalStockholderEquity'),
            'net_tangible_assets': check_value(int, row, 'netTangibleAssets')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(balance_sheet_quarterly)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(balance_sheet_quarterly.c.symbol_id) \
            .where(and_(balance_sheet_quarterly.c.symbol_id == row['symbol_id'],
                        balance_sheet_quarterly.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = balance_sheet_quarterly.update() \
                .where(and_(balance_sheet_quarterly.c.symbol_id == row['symbol_id'],
                            balance_sheet_quarterly.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = balance_sheet_quarterly.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


cashflow_annual = sqlalchemy.Table(  # Yahoo_cashflowStatementHistory(Base):
    'cashflow_annual',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("net_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("depreciation", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_netincome", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_liabilities", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_operating_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cash_from_operating_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("capital_expenditures", sqlalchemy.BigInteger),
    sqlalchemy.Column("investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cashflows_from_investing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_borrowings", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_cashflows_from_financing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cash_from_financing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("effect_of_exchange_rate", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_in_cash", sqlalchemy.BigInteger),
    sqlalchemy.Column("repurchase_of_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_account_receivables", sqlalchemy.BigInteger),
    sqlalchemy.Column("dividends_paid", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class CashflowAnnual:  # Yahoo_cashflowStatementHistory(Base):
    tablename = 'cashflow_annual'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'net_income': check_value(int, row, 'netIncome'),
            'depreciation': check_value(int, row, 'depreciation'),
            'change_to_netincome': check_value(int, row, 'changeToNetincome'),
            'change_to_liabilities': check_value(int, row, 'changeToLiabilities'),
            'change_to_operating_activities': check_value(int, row, 'changeToOperatingActivities'),
            'total_cash_from_operating_activities': check_value(int, row, 'totalCashFromOperatingActivities'),
            'capital_expenditures': check_value(int, row, 'capitalExpenditures'),
            'investments': check_value(int, row, 'investments'),
            'total_cashflows_from_investing_activities': check_value(int, row, 'totalCashflowsFromInvestingActivities'),
            'net_borrowings': check_value(int, row, 'netBorrowings'),
            'other_cashflows_from_financing_activities': check_value(int, row, 'otherCashflowsFromFinancingActivities'),
            'total_cash_from_financing_activities': check_value(int, row, 'totalCashFromFinancingActivities'),
            'effect_of_exchange_rate': check_value(int, row, 'effectOfExchangeRate'),
            'change_in_cash': check_value(int, row, 'changeInCash'),
            'repurchase_of_stock': check_value(int, row, 'repurchaseOfStock'),
            'change_to_account_receivables': check_value(int, row, 'changeToAccountReceivables'),
            'dividends_paid': check_value(int, row, 'dividendsPaid')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(cashflow_annual)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(cashflow_annual.c.symbol_id)\
                   .where(and_(cashflow_annual.c.symbol_id == row['symbol_id'],
                               cashflow_annual.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = cashflow_annual.update()\
                    .where(and_(cashflow_annual.c.symbol_id == row['symbol_id'],
                                cashflow_annual.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = cashflow_annual.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


cashflow_quarterly = sqlalchemy.Table(  # Yahoo_cashflowStatementHistoryQuarterly(Base):
    'cashflow_quarterly',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("net_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("depreciation", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_netincome", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_liabilities", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_operating_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cash_from_operating_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("capital_expenditures", sqlalchemy.BigInteger),
    sqlalchemy.Column("investments", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cashflows_from_investing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_borrowings", sqlalchemy.BigInteger),
    sqlalchemy.Column("other_cashflows_from_financing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_cash_from_financing_activities", sqlalchemy.BigInteger),
    sqlalchemy.Column("effect_of_exchange_rate", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_in_cash", sqlalchemy.BigInteger),
    sqlalchemy.Column("repurchase_of_stock", sqlalchemy.BigInteger),
    sqlalchemy.Column("change_to_account_receivables", sqlalchemy.BigInteger),
    sqlalchemy.Column("dividends_paid", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class CashflowQuarterly:  # Yahoo_cashflowStatementHistoryQuarterly(Base):
    tablename = 'cashflow_quarterly'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'net_income': check_value(int, row, 'netIncome'),
            'depreciation': check_value(int, row, 'depreciation'),
            'change_to_netincome': check_value(int, row, 'changeToNetIncome'),
            'change_to_liabilities': check_value(int, row, 'changeToLiabilities'),
            'change_to_operating_activities': check_value(int, row, 'changeToOperatingActivities'),
            'total_cash_from_operating_activities': check_value(int, row, 'totalCashFromOperatingActivities'),
            'capital_expenditures': check_value(int, row, 'capitalExpenditures'),
            'investments': check_value(int, row, 'investments'),
            'total_cashflows_from_investing_activities': check_value(int, row, 'totalCashflowsFromInvestingActivities'),
            'net_borrowings': check_value(int, row, 'netBorrowings'),
            'other_cashflows_from_financing_activities': check_value(int, row, 'otherCashflowsFromFinancingActivities'),
            'total_cash_from_financing_activities': check_value(int, row, 'totalCashFromFinancingActivities'),
            'effect_of_exchange_rate': check_value(int, row, 'effectOfExchangeRate'),
            'change_in_cash': check_value(int, row, 'changeInCash'),
            'repurchase_of_stock': check_value(int, row, 'repurchaseOfStock'),
            'change_to_account_receivables': check_value(int, row, 'changeToAccountReceivables'),
            'dividends_paid': check_value(int, row, 'dividendsPaid')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(cashflow_quarterly)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(cashflow_quarterly.c.symbol_id)\
                   .where(and_(cashflow_quarterly.c.symbol_id == row['symbol_id'],
                               cashflow_quarterly.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = cashflow_quarterly.update()\
                    .where(and_(cashflow_quarterly.c.symbol_id == row['symbol_id'],
                                cashflow_quarterly.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = cashflow_quarterly.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


income_annual = sqlalchemy.Table(  # Yahoo_incomeStatementHistory(Base):
    'income_annual',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("total_revenue", sqlalchemy.BigInteger),
    sqlalchemy.Column("cost_of_revenue", sqlalchemy.BigInteger),
    sqlalchemy.Column("gross_profit", sqlalchemy.BigInteger),
    sqlalchemy.Column("research_development", sqlalchemy.BigInteger),
    sqlalchemy.Column("selling_general_administrative", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_operating_expenses", sqlalchemy.BigInteger),
    sqlalchemy.Column("operating_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_other_income_expense_net", sqlalchemy.BigInteger),
    sqlalchemy.Column("ebit", sqlalchemy.BigInteger),
    sqlalchemy.Column("interest_expense", sqlalchemy.BigInteger),
    sqlalchemy.Column("income_before_tax", sqlalchemy.BigInteger),
    sqlalchemy.Column("income_tax_expense", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income_from_continuing_ops", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income_applicable_to_common_shares", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class IncomeAnnual:  # Yahoo_incomeStatementHistory(Base):
    tablename = 'income_annual'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'total_revenue': check_value(int, row, 'totalRevenue'),
            'cost_of_revenue': check_value(int, row, 'costOfRevenue'),
            'gross_profit': check_value(int, row, 'grossProfit'),
            'research_development': check_value(int, row, 'researchDevelopment'),
            'selling_general_administrative': check_value(int, row, 'sellingGeneralAdministrative'),
            'total_operating_expenses': check_value(int, row, 'totalOperatingExpenses'),
            'operating_income': check_value(int, row, 'operatingIncome'),
            'total_other_income_expense_net': check_value(int, row, 'totalOtherIncomeExpenseNet'),
            'ebit': check_value(int, row, 'ebit'),
            'interest_expense': check_value(int, row, 'interestExpense'),
            'income_before_tax': check_value(int, row, 'incomeBeforeTax'),
            'income_tax_expense': check_value(int, row, 'incomeTaxExpense'),
            'net_income_from_continuing_ops': check_value(int, row, 'netIncomeFromContinuingOps'),
            'net_income': check_value(int, row, 'netIncome'),
            'net_income_applicable_to_common_shares': check_value(int, row, 'netIncomeApplicableToCommonShares')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(income_annual)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(income_annual.c.symbol_id)\
                   .where(and_(income_annual.c.symbol_id == row['symbol_id'],
                               income_annual.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = income_annual.update()\
                    .where(and_(income_annual.c.symbol_id == row['symbol_id'],
                                income_annual.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = income_annual.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id

#######################################################################################################################


income_quarterly = sqlalchemy.Table(  # Yahoo_incomeStatementHistoryQuarterly(Base):
    'income_quarterly',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("end_date", sqlalchemy.DateTime, primary_key=True, nullable=False),
    sqlalchemy.Column("total_revenue", sqlalchemy.BigInteger),
    sqlalchemy.Column("cost_of_revenue", sqlalchemy.BigInteger),
    sqlalchemy.Column("gross_profit", sqlalchemy.BigInteger),
    sqlalchemy.Column("research_development", sqlalchemy.BigInteger),
    sqlalchemy.Column("selling_general_administrative", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_operating_expenses", sqlalchemy.BigInteger),
    sqlalchemy.Column("operating_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("total_other_income_expense_net", sqlalchemy.BigInteger),
    sqlalchemy.Column("ebit", sqlalchemy.BigInteger),
    sqlalchemy.Column("interest_expense", sqlalchemy.BigInteger),
    sqlalchemy.Column("income_before_tax", sqlalchemy.BigInteger),
    sqlalchemy.Column("income_tax_expense", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income_from_continuing_ops", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income", sqlalchemy.BigInteger),
    sqlalchemy.Column("net_income_applicable_to_common_shares", sqlalchemy.BigInteger),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class IncomeQuarterly:  # Yahoo_incomeStatementHistoryQuarterly(Base):
    tablename = 'income_quarterly'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'end_date': check_value('Datetime', row, 'endDate'),
            'total_revenue': check_value(int, row, 'totalRevenue'),
            'cost_of_revenue': check_value(int, row, 'costOfRevenue'),
            'gross_profit': check_value(int, row, 'grossProfit'),
            'research_development': check_value(int, row, 'researchDevelopment'),
            'selling_general_administrative': check_value(int, row, 'sellingGeneralAdministrative'),
            'total_operating_expenses': check_value(int, row, 'totalOperatingExpenses'),
            'operating_income': check_value(int, row, 'operatingIncome'),
            'total_other_income_expense_net': check_value(int, row, 'totalOtherIncomeExpenseNet'),
            'ebit': check_value(int, row, 'ebit'),
            'interest_expense': check_value(int, row, 'interestExpense'),
            'income_before_tax': check_value(int, row, 'incomeBeforeTax'),
            'income_tax_expense': check_value(int, row, 'incomeTaxExpense'),
            'net_income_from_continuing_ops': check_value(int, row, 'netIncomeFromContinuingOps'),
            'net_income': check_value(int, row, 'netIncome'),
            'net_income_applicable_to_common_shares': check_value(int, row, 'netIncomeApplicableToCommonShares')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(income_quarterly)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(income_quarterly.c.symbol_id)\
                   .where(and_(income_quarterly.c.symbol_id == row['symbol_id'],
                               income_quarterly.c.end_date == row['end_date']))
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = income_quarterly.update()\
                    .where(and_(income_quarterly.c.symbol_id == row['symbol_id'],
                                income_quarterly.c.end_date == row['end_date'])).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = income_quarterly.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id


#######################################################################################################################

events = sqlalchemy.Table(  # Yahoo_calendarEvents(Base):
    'events',
    metadata,
    sqlalchemy.Column("symbol_id", sqlalchemy.String(12), primary_key=True, nullable=False),
    sqlalchemy.Column("symbol", sqlalchemy.String(12)),
    sqlalchemy.Column("dividend_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("ex_dividend_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("earnings_average", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("earnings_low", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("earnings_high", sqlalchemy.Numeric(12, 6)),
    sqlalchemy.Column("revenue_average", sqlalchemy.BigInteger),
    sqlalchemy.Column("revenue_low", sqlalchemy.BigInteger),
    sqlalchemy.Column("revenue_high", sqlalchemy.BigInteger),
    sqlalchemy.Column("start_earnings_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("end_earnings_date", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_created", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("dt_updated", sqlalchemy.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
)


class Events:  # Yahoo_calendarEvents(Base):
    tablename = 'events'

    @classmethod
    def process_values(cls, row):
        record = {
            'symbol_id': row['symbol_id'],
            'symbol': row['symbol'],
            'ex_dividend_date': check_value('Datetime', row, 'exDividendDate'),
            'dividend_date': check_value('Datetime', row, 'dividendDate'),
            'earnings_average': check_value(float, row, 'earningsAverage'),
            'earnings_low': check_value(float, row, 'earningsLow'),
            'earnings_high': check_value(float, row, 'earningsHigh'),
            'revenue_average': check_value(int, row, 'revenueAverage'),
            'revenue_low': check_value(int, row, 'revenueLow'),
            'revenue_high': check_value(int, row, 'revenueHigh'),
            'start_earnings_date': check_value('Datetime', row, 'start_earningsDate'),
            'end_earnings_date': check_value('Datetime', row, 'end_earningsDate')
        }
        return record

    @classmethod
    async def get_all(cls, to_df=True):
        query = select(events)
        result = await database.fetch_all(query)
        return check_result(result, to_df)

    @classmethod
    async def create_or_update(cls, row):
        query_id = select(events.c.symbol_id)\
                   .where(events.c.symbol_id == row['symbol_id'])
        row_id = await database.fetch_one(query_id)
        if row_id:
            query = events.update()\
                    .where(events.c.symbol_id == row['symbol_id']).values(**row)
            row_id = await database.execute(query=query)
        else:
            query = events.insert().values(**row)
            row_id = await database.execute(query=query)
        return row_id
