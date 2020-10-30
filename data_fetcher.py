from tda.auth import easy_client
from tda.streaming import StreamClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime

import os
import robin_stocks as rh
import pandas as pd
import asyncio

from utils import get_cached_option_file_name, list_all_files_in_dir

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

LevelOneEquityFields = StreamClient.LevelOneEquityFields


class RobinFetcher:
    def __init__(self):
        self.cache_dir = './cache/rh/'

        email = os.getenv('ROBINHOOD_EMAIL')
        password = os.getenv('ROBINHOOD_PASSWORD')

        rh.login(email, password)

        cached_files = list_all_files_in_dir(self.cache_dir)

        self.options_data = dict()

        for file in cached_files:
            underlying, strike, option_type, exp = file.split('_')
            exp = exp.split('.')[0]
            strike = float(strike)
            self.add_option_to_cache(underlying, exp, strike, option_type='call')
            self.options_data[underlying][exp][strike][option_type] = pd.read_csv(self.cache_dir + file, index_col=0)

    def add_option_to_cache(self, symbol, exp, strike, option_type='call'):
        if symbol not in self.options_data:
            self.options_data[symbol] = dict()

        if exp not in self.options_data[symbol]:
            self.options_data[symbol][exp] = dict()

        if strike not in self.options_data[symbol][exp]:
            self.options_data[symbol][exp][strike] = dict()

        if option_type not in self.options_data[symbol][exp][strike]:
            self.options_data[symbol][exp][strike][option_type] = None

        return self.options_data[symbol][exp][strike][option_type]

    def get_historical_options(self, symbol, exp, strike, option_type='call', interval='5minute', span='week', refresh=False):
        option = self.add_option_to_cache(symbol, exp, strike, option_type)

        if refresh or option is None:
            self.options_data[symbol][exp][strike][option_type] = self.fetch_historical_options(symbol, exp, strike, option_type,
                                                                                  interval, span)
            file_name = self.cache_dir + get_cached_option_file_name(symbol, datetime.strptime(exp, "%Y-%m-%d"), strike, option_type)
            self.options_data[symbol][exp][strike][option_type].to_csv(file_name)

            print('Cached file, ', file_name)

        return self.options_data[symbol][exp][strike][option_type]

    def _clean(self, df):
        df.dropna(inplace=True)
        df.rename(columns={'begins_at': 'datetime', 'open_price': 'open', 'close_price': 'close', 'high_price': 'high',
                           'low_price': 'low'}, inplace=True)

        df["datetime"] = df["datetime"].astype('datetime64').dt.tz_localize('EST')
        df['low'] = df['low'].astype(float)
        df['high'] = df['high'].astype(float)
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df.drop(columns=['session', 'interpolated', 'symbol'], inplace=True)
        return df

    def fetch_historical_options(self, symbol, exp, strike, option_type='call',
                               interval='day', span='year', bounds='regular'):
        print('-- Fetching Option: {} {} {} {} --'.format(symbol, strike, option_type, exp))
        historical_data = rh.get_option_historicals(symbol, exp, strike, option_type, interval, span,
                                                    bounds)
        df = pd.DataFrame.from_records(historical_data)
        return self._clean(df)

    def get_historical_stock(self, symbol, interval='day', span='year', bounds='regular'):
        print('-- Fetching Stock: {} --'.format(symbol))
        historical_data = rh.get_stock_historicals(symbol, interval, span, bounds)
        df = pd.DataFrame.from_records(historical_data)
        return self._clean(df)


class TDFetcher:
    def __init__(self, client):
        self.client = client
        self.td_client = client.client

    def _clean(self, df):
        df.dropna(inplace=True)
        df["datetime"] = df["datetime"].dt.tz_localize('EST')
        return df

    """
    PERIOD_TYPE_VALUES = ('day', 'month', 'year', 'ytd')
    FREQUENCY_TYPE_VALUES = ('minute', 'daily', 'weekly', 'monthly')
    """
    def get_historical_stock(self, symbol, period_type='day', period=10, frequency_type='minute', frequency=1):
        print('-- Fetching Stock: {} --'.format(symbol))
        df = self.td_client.historyDF(symbol, periodType=period_type, period=period, frequencyType=frequency_type, frequency=frequency)
        return self._clean(df)

    def start_stream(self, callback):
        client = easy_client(
            api_key=self.client.client_id,
            redirect_uri=self.client.redirect_url,
            token_path='/Users/sibtain/Documents/td_token.pickle')

        stream_client = StreamClient(client, account_id=self.client.account_id)

        tickers = self.client.get_portfolio_tickers()

        def order_book_handler(msg):
            callback(msg)

        async def read_stream():
            await stream_client.login()
            await stream_client.quality_of_service(StreamClient.QOSLevel.DELAYED)
            await stream_client.level_one_equity_subs(tickers,
                fields=[LevelOneEquityFields.SYMBOL, LevelOneEquityFields.BID_PRICE, LevelOneEquityFields.ASK_PRICE,
                        LevelOneEquityFields.ASK_SIZE, LevelOneEquityFields.BID_SIZE, LevelOneEquityFields.TOTAL_VOLUME,
                        LevelOneEquityFields.TRADE_TIME, LevelOneEquityFields.HIGH_PRICE,
                        LevelOneEquityFields.LOW_PRICE,
                        LevelOneEquityFields.VOLATILITY, LevelOneEquityFields.NET_CHANGE])


            stream_client.add_level_one_equity_handler(order_book_handler)

            while True:
                await stream_client.handle_message()

        asyncio.get_event_loop().run_until_complete(read_stream())