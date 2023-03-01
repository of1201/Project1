"""
@ auther: Yiling Qi
"""

import socket  # establish connection between two processes
import time
import argparse  # parse cmd arguments
import threading  # multi-threading
import re
import sys
import csv
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
import json  # encode/decode
import requests  # v2.28.2
import finnhub  # v2.4.15

lock = threading.Lock()

class communication:
    """
    get server arguments and client inputs and perform operations accordingly

    # interact between controller (database, trading_strategy, genReport), server_parser, and client_parser
    """

    def __init__(self):
        self.lock = lock
        self.controller = None
        self.server_parser = server_parser()  # parse server inputs and create socket for the server
        self.client_parser = client_parser()  # parse clients inputs and send result back to clients

    def launch_server(self):
        """
        make server a socket that can receive inputs from multiple clients and perform tasks for each client
        """

        # parse server arguments
        port, tickers, sampling_period = self.server_parser.get_arguments()

        # create controller object
        self.controller = controller(tickers, sampling_period)

        # run server
        self.controller.update_price()  # constantly updating stock price in database class
        self.controller.generate_report(latest=False, save=True)  # generate the report according to price and save to local

    def build_tcp(self):
        """
        make server a socket that can receive inputs from multiple clients and perform tasks for each client
        """

        sock = self.server_parser.create_socket()  # server socket object

        conn_list = []
        while True:  # continuously listen for new connections

            # ensure that server can accept new connection
            if not conn_list:
                print('waiting for a connection')
            connection, client_address = sock.accept()

            # establish connection with new client
            if client_address not in conn_list:
                conn_list.append(client_address)  # store each new client connection in a list
                # build thread here so the server can deal with multiple clients the same time
                thread_client_interaction = threading.Thread(target=self.tcp_interaction, args=(connection,))
                thread_client_interaction.start()
                print('Client connected. Client ip:', client_address)
            # if client address already exists, server would keep waiting for another client
            else:
                print('Client already connected.')

    def tcp_interaction(self, connection):
        """
        perform task according to client's inputs
        """

        while True:  # server can continuously process on client's other inputs
            try:
                # parse client input and store as string
                client_input = self.client_parser.get_arguments(connection)

                # case 1: clients input a data to request record from report
                if re.match(r"^data \d\d\d\d-\d\d-\d\d-\d\d:\d\d$", client_input):  # ^ indicates it is beginning of the string
                    # query records from report
                    query = self.client_query_data(client_input)

                    # send query results back to clients
                    self.client_parser.send_client(connection, query)

                # case 2: if datetime is not specified by client, just return latest records
                elif re.match(r"^data$", client_input):  # & indicates this is the end of string
                    # query records from report
                    query = self.client_query_data(client_input, False)

                    # send query results back to clients
                    self.client_parser.send_client(connection, query)

                # case 3: clients ask to delete one ticker
                elif re.match(r"^delete .", client_input):  # . means there can have any characters
                    self.lock.acquire()  # lock for modifying

                    # delete tickers
                    msg = self.client_delete_tickers(client_input)

                    # send message back to clients
                    self.client_parser.send_client(connection, msg)

                    self.lock.release()

                # case 4: clients ask to add one ticker
                elif re.match(r"^add .", client_input):
                    self.lock.acquire()  # lock for modifying

                    # add tickers
                    msg = self.client_add_tickers(client_input)

                    # send message back to clients
                    self.client_parser.send_client(connection, msg)

                    self.lock.release()

                # case 5: clients ask to regenerate report and save to local
                elif client_input.lower() == 'report':
                    # generate latest report and save
                    msg = self.client_generate_report()

                    # send message back to clients
                    self.client_parser.send_client(connection, msg)

                # case 6: clients input unrecognized arguments
                else:
                    # send message back to clients
                    msg = 'unrecognized inputs'
                    self.client_parser.send_client(connection, msg)

            except Exception as e:  # something wrong when interacting with clients
                # release the lock if there is any
                if self.lock.acquire(False):
                    self.lock.release()

                # send error message back to clients
                msg = str(e)
                self.client_parser.send_client(connection, msg)

                connection.close()  # close current connection with the client
                break

    def client_query_data(self, client_input, date_given=True):
        """
        query from report (stored in controller class) for latest price and signal available as of the time specified
        """

        if date_given:  # if clients input a date
            # convert input to datetime
            query_time_str = (client_input.split(' '))[1]
            query_time = dt.strptime(query_time_str, '%Y-%m-%d-%H:%M')

            current_time = dt.now()  # current time

            # query records from report
            try:
                if query_time > current_time:  # specifying time in the future, returns the latest data
                    query = self.controller.query_data(current_time)
                    query.append('(specifying time in the future, returns the latest data)')
                else:
                    query = self.controller.query_data(query_time)
            except:
                query = ['Server has no data', '(specifying time before any data exists)']

        else:  # if no date is inputted by clients, just return the latest records
            current_time = dt.now()  # current time
            query = self.controller.query_data(current_time)
            query.append('(calling data without time returns the latest data)')

        return query

    def client_add_tickers(self, client_input):
        """
        add tickers according to clients inputs
        """

        # get ticker string
        ticker_added = (client_input.split(' '))[1]

        msg = 0  # success
        try:
            self.controller.add_ticker(ticker_added)
        except Exception as e:
            if str(e) == "invalid ticker":
                msg = 2  # invalid ticker
            else:
                msg = 1  # server error

        return msg

    def client_delete_tickers(self, client_input):
        """
        delete tickers according to clients inputs
        """

        # get ticker string
        ticker_deleted = (client_input.split(' '))[1]

        msg = 0  # success
        try:
            self.controller.delete_ticker(ticker_deleted)
        except Exception as e:
            if str(e) == "ticker not found":
                msg = 2  # ticker not found
            else:
                msg = 1  # server error

        return msg

    def client_generate_report(self):
        """
        generate the latest report and save
        """

        try:
            self.controller.generate_report(latest=True, save=True)
            msg = 'report generated'
        except:
            msg = 'server failed to generate report'

        return msg

class server_parser:
    """
    get user inputs for the server
    """

    def __init__(self):
        self.parser = argparse.ArgumentParser()  # parser object
        self.args = None  # arguments object
        self.port = None
        self.tickers = None
        self.sampling_period = None
        self.host = '127.0.0.1'  # localhost

    def get_arguments(self):
        """
        parse user inputs for server
        """

        self.parser.add_argument('--tickers', nargs="*", type=str, default=['AAPL', 'MSFT', 'TOST'])
        self.parser.add_argument('--port', type=int, default=8000)
        self.parser.add_argument('--sampling', type=int, default=5, choices=[5, 15, 30, 60])
        self.args = self.parser.parse_args()
        self.port = self.args.port
        self.tickers = self.args.tickers
        self.sampling_period = self.args.sampling

        return self.port, self.tickers, self.sampling_period

    def create_socket(self):
        """
        make the local server available over the internet by making it a socket
        """

        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # print server ip info
        server_address = (self.host, self.port)
        print('starting up on {} port {}'.format(*server_address))
        print()

        # Bind the socket to a specific port
        # bind an empty string so the server listens on all network devices within the LAN
        sock.bind(('', self.port))

        # Listen for incoming connections
        sock.listen(10)  # 10 clients can be in the queue

        return sock

class client_parser:
    """
    parse user inputs from clients
    """

    def __init__(self):
        pass

    def get_arguments(self, connection):
        """
        parse clients arguments
        """

        bdata = connection.recv(4096)  # receive clients arguments in bytes. Specifying max bytes to receive
        data = ((str(bdata)).split('\''))[1]  # store clients arguments in string
        print('client input: {}'.format(data))

        return data

    def send_client(self, connection, result):
        """
        send results back to clients with results converted to bytes
        """

        return connection.sendall(json.dumps(result).encode())


class database:
    """
    fetch historical price from Source1 and update real-time stock price from Source2

    :param tickers: list of stock tickers
    :type tickers: List of Strings
    :param sampling_period: one of (5, 15, 30, 60), with unit in minutes
    :type sampling_period: Int
    """
    def __init__(self, tickers, sampling_period):
        self.session = requests.Session()
        self.lock = lock
        self.tickers = tickers
        self.sampling_period = sampling_period
        self.key_source1 = 'QILARUF4KL7NB71W'
        self.key_source2 = 'cfb305pr01qrdg3nceu0cfb305pr01qrdg3nceug'
        self.price = None
        self.latest_time = None
        self.source2_time = None
        self.price_historical = None
        self.price_realtime = None
        self.status = None
        # self.regenerate_db_flag = False

    @staticmethod
    def fetch_source1(ticker, sampling, key, session):
        """
        create a df containing stock ticker's historical close price
        """
        # connect with source 1 and fetch ticker content
        input = (ticker, sampling, key)
        CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}' \
                  '&interval={}min&slice=year1month1&apikey={}'.format(*input)
        download = session.get(CSV_URL)
        decoded_content = download.content.decode('utf-8')
        csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
        list_source1_content = list(csv_reader)  # source1 ticker content list
        header, content = list_source1_content[0], list_source1_content[1:]  # get header and content

        # store only close price and time in dataframe
        df_price_source1 = pd.DataFrame(columns=header, data=content)[['time', 'close']]
        df_price_source1 = df_price_source1.rename(columns={'close': ticker})  # rename column 'close' to ticker name
        df_price_source1['time'] = pd.to_datetime(df_price_source1['time'], format='%Y-%m-%d %H:%M:%S')  # reformat time column

        # check if the tickers is valid:
        if df_price_source1.empty:
            raise Exception("invalid ticker")
        elif not isinstance(df_price_source1, pd.DataFrame):
            raise Exception('database error (source 1)!')

        return df_price_source1

    @staticmethod
    def fetch_source2(ticker, key):
        """
        create a df containing only stock ticker's real time close price
        """
        # connect with source 2 and fetch ticker real-time content
        finnhub_client = finnhub.Client(api_key=key)
        dict_source2_content = finnhub_client.quote(ticker)  # source2 ticker content dict

        # only store time and close price
        source2_time = dt.fromtimestamp(dict_source2_content['t'])  # time for the latest price
        source2_price = dict_source2_content['c']  # get the latest close price
        source2_time = dt.strptime(dt.strftime(source2_time, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')  # reformat time
        # store the ticker's real-time price and time in dataframe
        dict_price_source2 = {'time': [source2_time], ticker: [source2_price]}
        df_price_source2 = pd.DataFrame(dict_price_source2)

        return df_price_source2, source2_time

    @staticmethod
    def fill_NA(df):
        """
        deal with dataframe's missing value using backward and forward fill

        :param df: df with time column in descending order
        """

        df.fillna(method='bfill', inplace=True)  # first backward fill (forward fill in time ascending order)
        df.fillna(method='ffill', inplace=True)  # then forward fill (backward fill in time ascending order)

        return df

    def fetch_price_historical(self):
        """
        get historical stock price from Source 1
        """

        # get historical price for all tickers from source 1 and join them
        df_price_source1 = pd.DataFrame(columns=['time'])
        with self.session as source1_session:
            for ticker in self.tickers:
                # fetch historical price for this ticker from source 1
                df_ticker = self.fetch_source1(ticker, self.sampling_period, self.key_source1, source1_session)
                # join each ticker's price
                df_price_source1 = df_price_source1.merge(df_ticker, how='outer', on='time')  # outer join on 'time' column

        # use backward then forward fill to deal with missing stock price after combining all tickers
        self.fill_NA(df_price_source1)  # df_price_source1 is in time descending order

        # latest time in the time series
        self.latest_time = df_price_source1['time'].loc[0]

        # cache the generated price.
        self.price = df_price_source1.copy()  # keep track of the updated price
        self.price_historical = df_price_source1.copy()  # only store the price obtained from source 1 (historical price)

        # track database status
        self.status = 'fetched historical'

    def append_realtime_price(self):
        """
        update stock price to include real time (latest) price from Source 2
        """

        # get real-time price for all tickers from source 2 and join them
        df_price_source2 = pd.DataFrame(columns=['time'])
        for ticker in self.tickers:
            # fetch real time price from source 2
            df_ticker, self.source2_time = self.fetch_source2(ticker, self.key_source2)
            # outer join each ticker's price on 'time' column
            df_price_source2 = df_price_source2.merge(df_ticker, how='outer', on='time')

        # check db status
        self.status = 'previous updated price available. No new updates yet'

        # update the price with the real-time price if it is available; if no, no update to the price
        if self.source2_time > self.latest_time:
            # use backward then forward fill to deal with missing stock price after combining all tickers
            self.fill_NA(df_price_source2)
            # only keep the latest row (ignoring the few seconds time delay when requesting for the latest price of different tickers)
            df_price_source2 = df_price_source2.iloc[:1]
            # cache the newly added price from source 2
            self.price_realtime = df_price_source2
            # cache the df that contains the updated stock price for all tickers if there are newer data available
            self.price = pd.concat([df_price_source2, self.price], ignore_index=True)
            # update latest time
            self.latest_time = self.source2_time
            # check db status
            self.status = "price updated to latest!"

    def update_price(self):
        """
        constantly update the stock price every "sampling_period" min (e.g. every 5 min if self.sampling_period = 5)
        """

        self.status = None  # reset status to None every time we re-update the price

        # get historical stock price
        try:
            self.lock.acquire()
            self.fetch_price_historical()  # fetch price from source 1
            # time.sleep(2)  # wait for fetching
            self.lock.release()
        except Exception as e:
            if str(e) == 'invalid ticker':
                raise e
            else:
                raise Exception('database error (source 1)!')

        # constantly update stock price every "sampling_period" minutes
        while True:
            time.sleep(60 * self.sampling_period)  # sleep for the sampling_period for this task
            try:
                self.lock.acquire()
                self.append_realtime_price()  # fetch and append price from source 2
                # time.sleep(2)
                self.lock.release()
            except:
                raise Exception("database error (source 2)!")

    def update_price_thread(self):
        """
        keep updating stock prices from source 1 and source 2 in multi-threading mode
        """

        thread_db_update = threading.Thread(target=self.update_price)  # create thread for updating price
        thread_db_update.daemon = True
        thread_db_update.start()

    def get_price(self, latest):
        """
        :return: price database representing historical or latest price
        """

        if latest:
            return self.price  # updated price
        else:
            return self.price_historical  # historical price from source 1

    # def regenerate_database(self):
    #     """
    #     mark new_db as True to signal that the tickers have been updated and the price database needs to be re-generated
    #     """
    #
    #     self.regenerate_db_flag = True

    def delete_from_db(self, ticker_deleted):
        """
        delete a ticker's data from the price database
        """

        if ticker_deleted in self.tickers:
            # if ticker is found in the current list, we delete its data from current price database
            self.tickers.remove(ticker_deleted)
            self.price.drop([ticker_deleted], axis=1, inplace=True)
        else:
            raise Exception("ticker not found")

    def add_to_db(self, ticker_added):
        """
        add a new ticker's data to the price database
        """

        # only add ticker if it does not exist in current tickers list already
        if ticker_added not in self.tickers:
            # check if ticker is valid
            self.fetch_source1(ticker_added, self.sampling_period, self.key_source1, self.session)

            # if this step is reached, then ticker is valid
            self.tickers.append(ticker_added)  # append the new ticker to tickers list
            self.update_price_thread()  # re-update price database according to new tickers list

class trading_strategy:
    """
    generate trading strategy analytics based on stock price

    :param tickers: list of stock tickers
    :type tickers: List of Strings
    :param sampling_period: one of (5, 15, 30, 60), with unit in minutes
    :type sampling_period: Int
    """

    def __init__(self):
        self.rolling_period = '24h'
        self.min_rolling_periods = 15
        self.tickers = None
        self.price = None
        self.rolling_mean = None
        self.rolling_std = None

    @staticmethod
    def index_time_ascd(df):
        """
        generate a df with index set to time in ascending order
        """

        df_update = df.set_index('time', inplace=False)  # set time to index
        df_update.sort_index(inplace=True)  # sort time index in ascending order

        return df_update

    def calc_rolling_stats(self, df_price_raw):
        """
        calc rolling stats: 24 hours rolling period: e.g. 3pm yesterday to 3pm today

        :param df_price_raw: df containing ticker's price (stored in database.price)
        :type df_price_raw: dataframe
        """

        # proprocess raw df_price
        df_price = self.index_time_ascd(df_price_raw)

        # calc rolling mean and std.
        self.rolling_mean = df_price.rolling(self.rolling_period, min_periods=self.min_rolling_periods).mean().astype(float)
        self.rolling_mean.dropna(inplace=True)
        self.rolling_std = df_price.rolling(self.rolling_period, self.min_rolling_periods).std().astype(float)
        self.rolling_std.dropna(inplace=True)

        # make sure that price index aligns with rolling stats index
        self.price = df_price.loc[self.rolling_mean.index].astype(float)

    def momentum_strategy(self, df_price):
        """
        Implement the momentum strategy to generate trading signal based on price, rolling mean and rolling std

        :param df_price: price database with time index of ascending order
        :return: df of trading signal and price
        """

        # get price, rolling mean, rolling std
        self.calc_rolling_stats(df_price)

        # implement momentum strategy
        signal_list = np.where(self.price > self.rolling_mean + self.rolling_std, 1,
                               np.where(self.price < self.rolling_mean - self.rolling_std, -1, np.nan))

        # trading signal cald'd at time t would apply to t+1 period, so adjust the index for this
        signal_list = signal_list[:-1]
        time_index = self.price.index[1:]

        # get tickers list
        self.tickers = list(self.price.columns.values)

        # generate signal dataframe and clean it
        df_signal = pd.DataFrame(signal_list, columns=self.tickers, index=time_index)

        # fill na: if mean-std < price < mean+std, signal(t+1) = signal(t), so use forward fill
        df_signal.fillna(method='ffill', inplace=True)
        df_signal.fillna(0, inplace=True)  # deal with the case where the earliest few records might have NA signals
        df_signal = df_signal.astype(int)

        return df_signal, self.price  # df_price has one more row than df_signal

    def calc_pnl(self, df_signal, df_price):
        """
        calc the pnl based on (price(t+1) - price(t)) * signal(t)

        :return: df of pnl
        """

        # price(t+1) - price(t)
        df_price_diff = df_price.diff()

        # signal starts at 5:20 ends at 20:00, price starts at 5:15 ends at 20:00
        # Therefore, we need price from 5:20 to 20:00 and signal from 5:20 to 20:00 - delta_t
        df_price_diff = df_price_diff[2:]  # adjust price time series
        df_signal = df_signal[:-1]  # adjust signal time series
        df_signal.index = df_price_diff.index  # reset index for signal

        # calc pnl
        df_pnl = df_price_diff * df_signal

        return df_pnl

class genReport:
    """
    generate trading strategy report based on stock price and strategy analytics
    """

    def __init__(self):
        pass

    @staticmethod
    def stack_df(df, name_second_col, name_third_col):
        """
        concatenate all columns in a dataframe

        :param df: dataframe with time as its index.
        :param name_second_col: string to rename for the second col of the stacked df
        :param name_third_col: string to rename for the third col of the stacked df
        :return: stacked dataframe with second and third column name as specified
        """

        df_output = df.stack().reset_index()
        df_output.rename(columns={df_output.columns[0]: "datetime", df_output.columns[1]: name_second_col, df_output.columns[2]: name_third_col}, inplace=True)

        return df_output

    def combine_df(self, df_pnl, df_signal, df_price):
        """
        combine df_price, df_pnl, df_signal to form the report dataframe
        """

        # concatenate all columns in each dataframe
        df_pnl = self.stack_df(df_pnl, 'ticker', 'pnl')
        df_signal = self.stack_df(df_signal, 'ticker', 'signal')
        df_price = self.stack_df(df_price, 'ticker', 'price')

        # join df_price, df_signal and df_pnl
        df_report = df_price.merge(df_signal.merge(df_pnl, on=['datetime', 'ticker']), on=['datetime', 'ticker'])
        # format datetime column
        df_report['datetime'] = df_report['datetime'].dt.strftime('%Y-%m-%d-%H:%M')

        return df_report

    def generate_report(self, df_pnl, df_signal, df_price, save):
        """
        generate the pnl, price, signal time series report for the requested tickers
        """

        # ensure correct formats for all input dataframes
        # align the time index for df_price, df_signal with df_pnl
        df_price = df_price.loc[df_pnl.index]
        df_signal = df_signal.loc[df_pnl.index]
        # adjust price and pnl's output to be two decimal places
        df_price = df_price.round(2)
        df_pnl = df_pnl.round(2)

        # concatenate all columns for df_price, df_signal and df_pnl
        df_report = self.combine_df(df_pnl, df_signal, df_price)

        # save the report to local
        if save:
            filepath = os.path.join(os.getcwd(), "report.csv")
            print("report saved at this address: ")
            print(filepath)
            print()
            df_report.to_csv(filepath, index=False)

        return df_report

    def query_data(self, df_report, query_time):
        """
        query from report for latest price and signal available as of the time specified
        """

        # convert 'datetime' column from string to datetime for comparison later
        df_report = df_report.copy()
        df_report['datetime'] = pd.to_datetime(df_report['datetime'])

        # get the latest info from the report as of the query_time
        latest_time_in_report = df_report['datetime'][df_report['datetime'] <= query_time].iloc[-1]
        latest_info = df_report.loc[df_report['datetime'] == latest_time_in_report][['ticker', 'price', 'signal']]
        # format the result
        latest_info['result'] = latest_info['ticker'].astype(str) + "   " + \
                                latest_info['price'].astype(str) + "," + \
                                latest_info["signal"].astype(str)
        result = latest_info['result'].to_list()

        return result

class controller:
    """
    This class is to coordinate database, trading_strategy and report
    """

    def __init__(self, tickers, sampling_period):
        self.tickers = tickers
        self.sampling_period = sampling_period
        # create database, trading_strategy and report objects
        self.database = database(tickers, sampling_period)
        self.trading_strategy = trading_strategy()
        self.report = genReport()
        self.price = None
        self.report_content = None

    def update_price(self):
        """
        constantly update stock price cache in database class in a multi-threading mode
        """

        self.database.update_price_thread()

    def get_price(self, latest):
        """
        get stock price cache from database class

        # this function must be run after function update_price has been run to generate the price database
        """

        # get latest price from database object
        while True:
            self.price = self.database.get_price(latest)
            # check if the price has been generated
            if isinstance(self.price, pd.DataFrame):
                # raise Exception("Please run update_price to generate the price first!")
                break

    def generate_report(self, latest, save):
        """
        generate the trading strategy analytics report based on updated price
        """

        # get price database from database class
        self.get_price(latest)

        # calc analytics to form the report
        df_signal, df_price = self.trading_strategy.momentum_strategy(self.price)  # get trading signals
        df_pnl = self.trading_strategy.calc_pnl(df_signal, df_price)  # get pnl

        # generate report
        self.report_content = self.report.generate_report(df_pnl, df_signal, df_price, save)

    def query_data(self, query_time=dt.now()):
        """
        query from report for latest price and signal available as of the time specified
        """

        self.generate_report(latest=True, save=False)

        return self.report.query_data(self.report_content, query_time)

    def delete_ticker(self, ticker_deleted):
        """
        delete ticker's data from the stock price cache in database class
        """

        self.database.delete_from_db(ticker_deleted)

    def add_ticker(self, ticker_added):
        """
        add ticker's data to the stock price cache in database class
        """

        self.database.add_to_db(ticker_added)



if __name__ == "__main__":

    communication = communication()  # instantiate the communication object

    communication.launch_server()  # server is running after receiving user input

    # make connection between server and clients (running as a thread)
    thread_tcp = threading.Thread(target=communication.build_tcp)
    thread_tcp.daemon = True  # run at background for a long running process
    thread_tcp.start()

    # make sure the server keeps running until keyboard interruption
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            sys.exit(0)

















