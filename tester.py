"""
underlying price history: TD api (5m, -1w)

to get options price history:

for now:
> Get from RH api (5m -1w, no need to save) and clean

later (1m, -4-5months):
> Get from omnieq -> ensure all options work and scraping works properly

> See if file already exists for ticker and defined timeline, scrape if not
> Return DF with same schema as options quote on TD API

"""
import pandas as pd
from data_fetcher import TDFetcher, RobinFetcher
from client import TdAccount
from utils import string_to_date, get_option_symbol, filter_df_by_date, TRANSACTIONS_COPY
from seller import Trader

pd.set_option('mode.chained_assignment', None)

td_client = TdAccount()
td_fetcher = TDFetcher(td_client.client)
robin_fetcher = RobinFetcher()

def get_positions():
    # transactions = td_client.get_transactions().copy()
    # transactions = pd.read_json(TRANSACTIONS_COPY).dropna()
    # transactions = transactions[transactions['positionEffect'] == 'OPENING'][transactions['optionExpirationDate'] > '2020-10-26T06:00:00+0000'].head(5)
    # print(transactions)
    # return transactions
    return td_client.get_active_positions().copy().head(3)


def get_train_test_data_option(option):
    exp = string_to_date(option['optionExpirationDate']).strftime('%Y-%m-%d')
    strike = float(option['description'].split(" ")[-2])
    data = robin_fetcher.get_historical_options(option['underlyingSymbol'], exp, strike)
    train_data = filter_df_by_date(data, from_date='2019-01-01', to_date=option['settlementDate'])
    test_data = filter_df_by_date(data, from_date=option['settlementDate'])
    option_symbol = get_option_symbol(option)
    test_data['symbol'], train_data['symbol'] = option_symbol, option_symbol
    return train_data, test_data


def get_train_test_data_stock(option):
    data = td_fetcher.get_historical_stock(option['underlyingSymbol'])
    train_data = filter_df_by_date(data, from_date='2019-01-01', to_date=option['settlementDate'])
    test_data = filter_df_by_date(data, from_date=option['settlementDate'])
    test_data['symbol'], train_data['symbol'] = option['underlyingSymbol'], option['underlyingSymbol']
    return train_data, test_data


def get_data(option):
    # get train,test underlying -1 year (train=-1year to date of purchase, test=purchase date to now)
    underlying_data = get_train_test_data_stock(option)

    # get train,test option -4months (or as far back as possible)
    option_data = get_train_test_data_option(option)

    return underlying_data, option_data


def get_test_data():
    initial_data = {}
    ticks = {} # [ [aapl_tick1, aapl_tick2], [xyz_tick1, xyz_tick2] ]
    for idx, buy in get_positions().iterrows():
        underlying, option = get_data(buy)
        underlying_symbol = buy['underlyingSymbol']
        option_symbol = get_option_symbol(buy)
        underlying_train, underlying_test = underlying
        option_train, option_test = option
        initial_data[option_symbol], initial_data[underlying_symbol] = option_train, underlying_train
        ticks[option_symbol], ticks[underlying_symbol] = option_test, underlying_test

    print('--- INITIAL DATA -- ', initial_data)
    return initial_data, ticks


class MockClient:
    def __init__(self, positions):
        self.td_client = {}
        self.positions = positions
        self.portfolio = None
        self.portfolio_df = None
        self.get_active_positions()

    def get_active_positions(self):
        # start with all buys, remove
        self.portfolio_df = self.positions
        self.portfolio = self.portfolio_df.to_dict(orient='records')

        return self.portfolio_df

    def sell_position(self, symbol):
        self.positions = self.positions[self.positions['symbol'] != symbol]
        self.get_active_positions()


initial_data, ticks = get_test_data()
client = MockClient(positions=get_positions())
trader = Trader(client=client, initial_data=initial_data)

# print('-- ticks -- ', ticks)
def begin_ticks(ticks):
    symbols = list(ticks.keys())
    num_ticks = max([ticks[symbols[i]].shape[0] for i in range(len(symbols))])

    for idx in range(num_ticks):
        for symbol in symbols:
            df = ticks[symbol]
            if idx < len(df):
                tick = df.iloc[idx]
                should_update = '_' not in symbol
                trader.receive_tick(tick, should_update)

    print('-- FINAL SYMBOLS -- ')
    trader.print_symbols()


begin_ticks(ticks)

