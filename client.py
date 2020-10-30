import tdameritrade as td
import pandas as pd
import os
import json

from data_fetcher import TDFetcher, RobinFetcher
from seller import Trader

pd.set_option('mode.chained_assignment', None)


class TdAccount:
    def __init__(self):
        self.client_id = os.getenv('TDAMERITRADE_CLIENT_ID')
        self.refresh_token = os.getenv('TDAMERITRADE_REFRESH_TOKEN')
        self.account_id = os.getenv('TDAMERITRADE_ACCOUNT_ID')
        self.redirect_url = 'http://localhost:8080'
        self.client = td.TDClient(client_id=self.client_id, refresh_token=self.refresh_token, account_ids=[self.account_id])
        self.portfolio = None
        self._working_trans_df = None
        self._initial_trans_df = None

    def auth(self):
        print(td.auth.authentication(self.client_id, self.redirect_url))

    def get_account(self):
        return self.client.accounts(positions=True)[self.account_id]['securitiesAccount']

    def get_active_positions(self):
        df = pd.DataFrame(data=self.get_account()['positions'])

        transactions_df = self.get_transactions(refresh=True)

        def find_in_transactions(position):
            symbol = position['symbol']
            amount = position['longQuantity'] if position['longQuantity'] is not 0 else position['shortQuantity']
            cost = amount * position['averagePrice'] * 100
            trans = transactions_df[
                (transactions_df['symbol'] == symbol) & (transactions_df['tradeType'] == 'BUY TRADE')]
            ans, i = None, 0
            while amount > 0 and cost > 0 and i < len(trans):
                curr_trans = trans.iloc[i]
                amount -= curr_trans['amount']
                cost += curr_trans['cost']

                if ans is None:
                    ans = curr_trans
                else:
                    ans['netAmount'] += curr_trans['netAmount']

                i -= 1

            return ans

        def clean(positions_df):
            df1 = pd.json_normalize(positions_df['instrument'])

            # print('-- COLS --', positions_df.columns, df1.columns)

            names = {}
            for col in df1.columns:
                if 'instrument.' in col:
                    name = col.split('.')[1]
                    names[col] = name
            df1.rename(columns=names, inplace=True)

            positions_df = positions_df.merge(df1, how='outer', left_index=True, right_index=True)
            positions_df.drop(
                columns=['settledLongQuantity', 'settledShortQuantity', 'instrument', 'maintenanceRequirement', 'cusip',
                         'type'], inplace=True)

            position_transactions = [None] * len(positions_df)
            transaction_keys = ['settlementDate', 'transactionDate', 'transactionId', 'netAmount', 'optionExpirationDate']

            for idx, row in positions_df.iterrows():
                transaction = find_in_transactions(row)
                if transaction is not None:
                    position_transactions[idx] = {key: transaction[key] for key in list(transaction.keys()) if
                                                  key in transaction_keys}
                else:
                    position_transactions[idx] = {key: None for key in transaction_keys}

                if '_' in row['symbol']:
                    position_transactions[idx]['strike'] = float(row['description'].split(" ")[-2])

            positions_df = positions_df.merge(pd.DataFrame.from_records(position_transactions), how='outer',
                                              left_index=True, right_index=True)
            positions_df['optionExpirationDate'] = positions_df['optionExpirationDate'].astype('datetime64').dt.tz_localize('EST')

            return positions_df

        self.portfolio_df = clean(df)
        self.portfolio = self.portfolio_df.to_dict(orient='records')

        return self.portfolio_df

    def get_portfolio_tickers(self):
        if not self.portfolio:
            self.get_active_positions()

        def build_symbol(position):
            return position['underlyingSymbol']

        return [build_symbol(position) for position in self.portfolio]

    def get_transactions(self, refresh=False):
        if self._working_trans_df is not None and not refresh:
            return self._working_trans_df

        if self._initial_trans_df is None or refresh:
            self._initial_trans_df = pd.DataFrame(data=self.client.transactions(type='ALL')[self.account_id])

        working_trans_df = self._initial_trans_df.copy()
        df2 = pd.json_normalize(working_trans_df['transactionItem'])
        df2.drop(columns=['accountId', 'instrument.cusip', 'instrument.assetType', 'instrument.type'], inplace=True)

        for col in df2.columns:
            if 'instrument.' in col:
                name = col.split('.')[1]
                df2[name] = df2[col]
                df2.drop(columns=[col], inplace=True)

        working_trans_df = working_trans_df[working_trans_df['type'] == 'TRADE']
        working_trans_df['tradeType'] = working_trans_df['description']
        working_trans_df.drop(
            columns=['type', 'description', 'cashBalanceEffectFlag', 'fees', 'clearingReferenceNumber', 'achStatus',
                     'transactionItem', 'subAccount'], inplace=True)
        working_trans_df = working_trans_df.merge(df2, how='outer', left_index=True, right_index=True)

        self._working_trans_df = working_trans_df

        return working_trans_df

    def sell_position(self, position):
        pass


def get_initial_data(client):
    initial_data = {}
    for idx, position in client.get_active_positions().iterrows():
        if '_' in position['symbol']:
            underlying_symbol = position['underlyingSymbol']
            option_symbol = position['symbol']
            exp = position['optionExpirationDate'].strftime('%Y-%m-%d')
            initial_data[underlying_symbol] = td_fetcher.get_historical_stock(underlying_symbol)
            initial_data[option_symbol] = rh_fetcher.get_historical_options(
                underlying_symbol, exp=exp, strike=position['strike'], option_type=position['putCall'].lower())
        else:
            initial_data[position['symbol']] = td_fetcher.get_historical_stock(position['symbol'])

    print('--- INITIAL DATA -- ', initial_data)
    return initial_data


def receive_stream_msg(msg):
    print(json.dumps(msg, indent=4))

client = TdAccount()

print(client.get_active_positions())

td_fetcher = TDFetcher(client)
rh_fetcher = RobinFetcher()

init_data = get_initial_data(client)

seller = Trader(client)

# td_fetcher.start_stream(receive_stream_msg)

