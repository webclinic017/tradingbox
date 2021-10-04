import requests
import alpaca_trade_api as tradeapi
from settings.settings import ALPACA_PAPER


class alpacaAPI():
    def __init__(self):
        self._api_key = ALPACA_PAPER['API_KEY']
        self._secret_key = ALPACA_PAPER['API_SECRET']
        self._base_url = ALPACA_PAPER['BASE_URL']

        self.api = tradeapi.REST(self._api_key,
                                 self._secret_key,
                                 self._base_url)

    def get_api_key(self):
        return self._api_key

    def get_secret_key(self):
        return self._secret_key

    def get_assets(self):
        return self.api.list_assets()

    def get_bars(self, symbol, freq='day', limit=None, start=None, until=None):
        return self.api.get_barset(symbol, freq, limit=limit, start=start, until=until)

    def get_quote(self, symbol):
        return self.api.get_last_quote(symbol)


    def get_orders(self):
        return self.api.list_orders()

    def get_symbol_orders(self):
        orders = self.get_orders()
        symbols = [order.symbol for order in orders]
        return symbols

    def get_positions(self):
        return self.api.list_positions()

    def get_position_symbols(self):
        positions = self.get_positions()
        return [position.symbol for position in positions]

    def get_position(self, symbol):
        try:
            ret = self.api.get_position(symbol)
            return ret
        except:
            return False

    def list_orders(self, status='all', after=None):
        if after:
            return self.api.list_orders(status=status, after=after)
        else:
            return self.api.list_orders(status=status)

    def cancel_orders(self):
        return self.api.cancel_all_orders()

    def limit_order(self, symbol, qty, side, limit_price, time_in_force='opg'):
        self.api.submit_order(
            symbol=symbol,
            qty=str(qty),
            side=side,
            type=limit_price,
            time_in_force=time_in_force,
            limit_price=limit_price
        )

    def bracket_limit_order(self, symbol, qty, side, limit_price, stop_price, tp_price, time_in_force='gtc'):
        self.api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='limit',
            time_in_force=time_in_force,
            order_class='bracket',
            limit_price = limit_price,
            stop_loss={'stop_price': stop_price },
            take_profit={'limit_price': tp_price }
        )

    def market_order(self, symbol, side='buy', qty=1, time_in_force='gtc'):
        self.api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force=time_in_force,
        )

    def trailing_order(self, symbol, side='sell', qty=1, time_in_force='gtc', trail_percent=None):
        self.api.submit_order(
            symbol = symbol,
            side = side,
            qty = qty,
            time_in_force = time_in_force,
            type='trailing_stop',
            trail_percent=trail_percent,
        )

    def close_all_positions(self):
        return self.api.close_all_positions()



    def get_date_bars(self, ticker_symbol, open_timestamp, close_timestamp):

        # set the url for pulling bar data
        base_url = 'https://data.alpaca.markets/v1/bars/minute'

        # set the request headers using our api key/secret
        request_headers = {'APCA-API-KEY-ID': self._api_key,'APCA-API-SECRET-KEY': self._secret_key}

        # set the request params for the next request
        request_params = {'symbols': ticker_symbol, 'limit': 1000, 'start': open_timestamp.isoformat(),
                          'end': close_timestamp.isoformat()}

        # get the response
        date_bars = requests.get(base_url, params=request_params, headers=request_headers).json()[ticker_symbol]

        # if the date on the response matches the closing date for the day, throw the candle away (since it technically happens after the close)
        if date_bars[-1]['t'] == int(close_timestamp.timestamp()):
            date_bars = date_bars[:-1]

        # return the bars for the date
        return date_bars