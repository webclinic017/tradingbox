from models.base import Data_Vendors, Exchanges, SubExchanges, Asset_Types, Scale, Sector_Types, Symbology
from models.fin_data import Summary, Financials, Fundamentals, Events, BalanceSheetQuarterly, BalanceSheetAnnual
from models.fin_data import CashflowAnnual, CashflowQuarterly, IncomeQuarterly, IncomeAnnual
from models.tickers import Sectors, Currencies, Tickers, Indices
from models.prices import Daily_Prices

TABLES ={
    'data_vendors': Data_Vendors,
    'exchanges': Exchanges,
    'asset_types': Asset_Types,
    'market_cap_scale': Scale,
    'sector_types': Sector_Types,
    'symbology': Symbology,
    'sub_exchanges': SubExchanges,
    'summary': Summary,
    'financials': Financials,
    'fundamentals': Fundamentals,
    'balancesheet_annual': BalanceSheetAnnual,
    'balancesheet_quarterly': BalanceSheetQuarterly,
    'cashflow_annual': CashflowAnnual,
    'cashflow_quarterly': CashflowQuarterly,
    'income_annual': IncomeAnnual,
    'income_quarterly': IncomeQuarterly,
    'events': Events,
    'sectors': Sectors,
    'currencies': Currencies,
    'tickers': Tickers,
    'indices': Indices,
    'daily_prices': Daily_Prices
}
"""
from models.base import Base, Data_Vendor, Exchange, SubExchange, Symbology, Asset_Type, Scale, Sector_Type
from models.tickers import Ticker, Sector, Currency, Indices
from models.prices import Daily_Prices
from models.fin_data import Summary, Financials, Fundamentals, Events, CashflowAnnual, CashflowQuarterly
from models.fin_data import IncomeAnnual, IncomeQuarterly, BalanceSheetQuarterly, BalanceSheetAnnual
# from database.models.ticker import Ticker



def get_database_class():
   # Return class reference mapped to table.
   # :return: Class reference or None.
    
    tables = {}
    models = Base.registry.mappers
    for model in models:
        c = model.entity
        tables[c.__table__.name] = c
    return tables

TABLES = get_database_class()
"""