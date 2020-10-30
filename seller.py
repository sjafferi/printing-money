import pandas as pd
import collections

"""

Goals of this service

Run every 5m starting 9:3am - 4pm

For each option in portfolio

1. Automate execution of sells based on:
        a. First significant sign of reversal
        b. Profit target
        c. Stop loss

2. Reduce losses / Increase risk adjusted gains / Reduce tension from manual selling

Testing:

Using previous buys, simulate replay of trading days, generate profit / loss.

Future adds:

1. Each buy / sell should have an associated strategy (day, swing, scalp, long-term)
"""

symbols = {} # tick -> symbol df

# develop actual strategies
# test and make sure it works with current portfolio
# back test properly
# implement real strat # 1 (news, relative volume + relative price)


def compute_sma(data, window):
    # simple moving average
    sma = data.rolling(window=window).mean()
    return sma


def compute_ema(data, span):
    ema = data.ewm(span=span, adjust=False).mean()
    return ema


def ema_sma_condition_generator(df, signals, conditions):
    # was below 200 EMA few days ago but today is above 200 EMA
    # possible long
    if (
        'EMA_200' in signals and
        (signals['EMA_200'].iloc[-5] > df['close'].iloc[-5]) and
        (signals['EMA_200'].iloc[-1] < df['close'].iloc[-1])
       ):
        conditions['ema_200_cross_over'] = True

    # price in vicinity 50 EMA
    # possible long or at least alert
    if (
        'EMA_50' in signals and
        ((signals['EMA_50'].iloc[-1] / df['close'].iloc[-1]) >= 0.98) and
        ((signals['EMA_50'].iloc[-1] / df['close'].iloc[-1]) <= 1.02)
       ):
        conditions['ema_50_vicinity'] = True

    # 50 ema is above 200 ema
    # possible long or at least alert
    if (
        'EMA_200' in signals and 'EMA_50' in signals and
        (signals['EMA_50'].iloc[-1] > signals['EMA_200'].iloc[-1] ) and
        (signals['EMA_50'].iloc[-2] < signals['EMA_200'].iloc[-2])
       ):
        conditions['ema_50_crosses_ema_200'] = True


def ema_sma_signal_generator(data, signals):
    for i in [5, 10, 50, 100, 250]:
        if len(data) < i:
            break
        signals['SMA_{}'.format(i)] = compute_sma(data['close'], i)
    for i in [5, 10, 50, 100, 250]:
        if len(data) < i:
            break
        signals['EMA_{}'.format(i)] = compute_ema(data['close'], i)


def news_signal_generator(data, signals):
    # if last fetch time was greater than threshold (5m)
    #    fetch news, gather sentiment
    pass


def volatility_signal_generator(data, signals):
    # implied vs. calculated volatility in the last few ticks
    pass


def volume_signal_generator(data, signals):
    # generate moving average volume
    pass


GENERATORS = {
    'signals': [ema_sma_signal_generator],
    'conditions': [ema_sma_condition_generator]
}


class Trader:
    def __init__(self, client, initial_data = {}, generators=GENERATORS):
        self.client = client
        self.symbols = {}
        self.generators = generators
        for position in self.client.portfolio:
            symbol = position['symbol']
            underlying_symbol = position['underlyingSymbol']
            # populate symbols
            self.add_symbol(symbol,
                            initial_data[symbol] if symbol in initial_data else pd.DataFrame(),
                            position)
            self.add_symbol(underlying_symbol,
                            initial_data[underlying_symbol] if underlying_symbol in initial_data else pd.DataFrame(),
                            position)

        self.print_symbols()

    def print_positions(self):
        print('-- CURRENT POSITIONS -- \n', self.client.get_active_positions().to_markdown())

    def print_symbols(self):
        self.print_positions()
        print('-- CURRENT SYMBOLS -- \n')
        for symbol in self.symbols.keys():
            info = self.symbols[symbol]
            print('-- Symbol: ', symbol, ' --')
            for key in info.keys():
                if key is 'signals':
                    print('- Signals -')
                    for signal in info[key].keys():
                        print('- ', signal, ' - ')
                        print(info[key][signal].head().to_markdown())
                elif key is 'data':
                    print(info[key].head().to_markdown())
                elif key is not 'position':
                    print('- ', key, ' -')
                    print(pd.DataFrame.from_records([info[key]]).to_markdown())

    def add_symbol(self, symbol, data, position = None):
        self.symbols[symbol] = {
            'position': position,
            'data': data,
            'signals': {},
            'conditions': {}  # if any are true, send order { should_execute, order_type, quantity, reason }
        }

    def refresh_portfolio(self):
        self.client.get_active_positions()
        self.print_positions()

        for position in self.client.portfolio:
            # populate symbols
            self.symbols[position['symbol']]['position'] = position

    def update_symbol(self, tick):
        symbol = tick['symbol']
        if symbol in self.symbols:
            self.symbols[symbol]['data'].append(tick)

    def generate_signals(self):
        for symbol in self.symbols.keys():
            if '_' in symbol:
                continue

            data = self.symbols[symbol]['data']
            signals = self.symbols[symbol]['signals']

            for signal_generator in self.generators['signals']:
                signal_generator(data, signals)

    def build_conditions(self):
        for symbol in self.symbols.keys():
            if '_' in symbol:
                continue
            data = self.symbols[symbol]['data']
            signals = self.symbols[symbol]['signals']
            conditions = self.symbols[symbol]['conditions']

            for condition_generator in self.generators['conditions']:
                condition_generator(data, signals, conditions)

    def send_orders(self):
        orders = []

        for symbol in self.symbols.copy().keys():
            if symbol not in self.symbols:
                continue

            conditions = self.symbols[symbol]['conditions']
            if len(conditions.keys()) > 0:
                option_symbol = self.symbols[symbol]['position'][
                    'symbol']  # position.symbol will always be option symbol

                if option_symbol in self.symbols:
                    orders.append((option_symbol, conditions))

        for symbol, conditions in orders:
            underlying = self.symbols[symbol]['position']['underlyingSymbol']

            print('-- SEND ORDER --', underlying, symbol, conditions)

            self.client.sell_position(symbol)
            del self.symbols[symbol]

            if not any([
                        option is not underlying and \
                        option.split('_')[0] is underlying \
                        for option in self.symbols.keys()
                    ]):
                del self.symbols[underlying]

        return orders

    def receive_tick(self, tick, update=True):
        self.update_symbol(tick)

        if update:
            self.generate_signals()
            self.build_conditions()
            if len(self.send_orders()) > 0:
                self.refresh_portfolio()

