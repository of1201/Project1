import socket
import time
import subprocess  # multi-processing
import argparse  # parse cmd arguments
import threading  # multi-threading
import re
import sys
import csv
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
import json
import requests  # v2.28.2
import finnhub  # v2.4.15


host = '127.0.0.1'  # localhost


def parser():
    '''
    # get the user inputs for each option
    # available options:
    --tickers: list of tickers; default=['AAPL','MSFT','TOST']
    --port: port number for the server. Default 8000
    --sampling: an integer representing the sampling period; choose from [5, 15, 30, 6] and default 5
    # return all options' inputs
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', nargs="*", type=str, default=['AAPL', 'MSFT', 'TOST'])
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--sampling', type=int, default=5, choices=[5, 15, 30, 60])
    args = parser.parse_args()

    return args

def tcp(connection, report, tickers, sampling):
    global lock
    while True:
        try:
            bdata = connection.recv(4096)
            #print('received {!r}'.format(bdata))
            data = ((str(bdata)).split('\''))[1]
            print('client input: {}'.format(data))

            lock.acquire()
            # case: clients request data for a date
            if re.match(r"^data \d\d\d\d-\d\d-\d\d-\d\d:\d\d$", data):
                timestr = (data.split(' '))[1]
                try:
                    time = dt.strptime(timestr, '%Y-%m-%d-%H:%M')
                except Exception as e:
                    connection.sendall('cannot convert input to date'.encode('utf-8'))

                cur_time = dt.now()
                # print(time)
                try:
                    if time > cur_time:  # specifying time in the future, returns the latest data
                        query = dataquery(report)
                        query.append('(specifying time in the future, returns the latest data)')
                    else:
                        query = dataquery(report, time)
                except:
                    query = ['Server has no data', '(specifying time before any data exists)']
                connection.sendall(json.dumps(query).encode())

                #print('data have date')
            elif re.match(r"^data$", data):  # If time is not specified, the client returns latest data
                #print(time)
                query = dataquery(report)
                query.append('(calling data without time returns the latest data)')
                connection.sendall(json.dumps(query).encode())
                #print('data no date')

            elif re.match(r"^delete .", data):
                ticker = (data.split(' '))[1]
                #print('delete TICKER', ticker)
                msg = 0
                if ticker in tickers:
                    try:
                        tickers.remove(ticker)
                        report = report[report.ticker != ticker]
                        msg = 0
                    except:
                        msg = 1
                else:
                    msg = 2

                if msg == 0:
                    connection.sendall(b'0')
                elif msg == 1:
                    connection.sendall(b'1')
                elif msg == 2:
                    connection.sendall(b'2')
                #print(tickers)

            elif re.match(r"^add .", data):
                ticker = (data.split(' '))[1]
                #print('add TICKER', ticker)
                msg = 0
                if ticker in tickers:
                    msg = 0
                else:
                    try:
                        tickers.append(ticker)
                        report = generate(tickers, sampling)
                        msg = 0
                    except Exception as e:
                        tickers.remove(ticker)
                        if str(e) == 'some tickers given are invalid':
                            msg = 2
                        else:
                            msg = 1

                if msg == 0:
                    connection.sendall(b'0')
                elif msg == 1:
                    connection.sendall(b'1')
                elif msg == 2:
                    connection.sendall(b'2')

                #print(tickers)

            elif data.lower() == 'report':
                try:
                    report = generate(tickers, sampling, True)
                    connection.sendall(b'report generated')
                except:
                    connection.sendall(b'server failed to generate report')
                #print('report')

            else:
                connection.sendall(b'unrecognized inputs')
                print('unrecognized input ' + data + ' from client')
            lock.release()
        except Exception as e:  # something wrong when interacting with clients
            print(e)
            if lock.acquire(False):
                lock.release()
            connection.close()
            break

def task(port, tickers, sampling):
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the port
    server_address = (host, port)
    print('starting up on {} port {}'.format(*server_address))
    print()
    # bind an empty string so the server listens on all network devices within the LAN
    sock.bind(('', port))

    # Listen for incoming connections
    sock.listen(10)  # 10 clients can be in the queue

    # getting stock data and generating csv file. Return the report
    try:
        report = generate(tickers, sampling, True)
    except Exception as e:
        if str(e) == 'some tickers given are invalid':
            print('invalid tickers')
        else:
            print('server error')
        sys.exit(1)

    conn_list = []
    conn_dt = {}
    while True:
        # Start a new session and wait for a connection
        print('waiting for a connection')
        connection, client_address = sock.accept()

        if client_address not in conn_list:
            conn_list.append(client_address)
            conn_dt[client_address] = connection
            # build thread here, so clients don't have to queue up
            t = threading.Thread(target=tcp, args=(connection, report, tickers, sampling))
            t.start()
        print('Client connected. Client ip:', client_address)


def dataquery(df, time=dt.now()):
    '''
    query the server for latest price and signal available as of the time specified.
    returns data like:
    AAPL    162.45,1
    MSFT    302.66,0

    '''

    df['datetime'] = pd.to_datetime(df['datetime'])
    time_final = df['datetime'][df['datetime'] <= time].iloc[-1]
    df_final = df.loc[df['datetime'] == time_final]
    df_final = df_final[['ticker', 'price', 'signal']]
    df_final['result'] = df['ticker'].astype(str) + "   " + df['price'].astype(str) + "," + df["signal"].astype(str)
    lst = df_final['result'].to_list()

    n_ticker = df['ticker'].nunique()
    if n_ticker > len(lst):
        lst.append('some tickets\' data for the input time are not available')

    return lst




def generate(tickers, sampling=5, save=False):
    '''
    connects to Source 1 and Source 2 and constructs a series of stock prices sampled at X-minute intervals, using all available historical data for the given tickers.
    then immediately computes a Boolean trading signal series for the entire price time series, and does profit & loss calculation. Then this information is written to a CSV file named report.csv in the format below.

    datetime, ticker, price, signal, pnl
    2022-04-25-11:00, AAPL, 421.04, -1, -0.02
    2022-04-25-11:00, MSFT, 132.95, 1, 0.06
    2022-04-25-11:05, AAPL, 421.09, -1, -0.05
    2022-04-25-11:05, MSFT, 132.98, 1, 0.03

    tickers: list of tickers
    sampling: one of (5, 15, 30, 60) with unit in minutes
    '''

    # read adjusted stock prices from Source 1
    key1 = 'QILARUF4KL7NB71W'
    key2 = 'cfb305pr01qrdg3nceu0cfb305pr01qrdg3nceug'

    def signal(df):
        if df['close'] > df['rolling_mean'] + df['rolling_std']:
            return 1
        elif df['close'] < df['rolling_mean'] - df['rolling_std']:
            return -1


    # Since source 1 API only limits 5 calls per minute, we just get the most recent period's prices for each ticker
    # imported stock prices are adjusted price

    # we use source 1's historical price to determine the strategy indicator
    # we assume for source 1 if we request data with 5min time frame at 11:02
    # it will give us the price for 11:00, 10:55, 10:50...
    # so we need to get the current price at 11:02. we only need to append the source 2 current price if the
    # latest price we can get from source 1 is between 9:30am to 4pm
    tickers_dict = {}
    with requests.Session() as s:
        for ticker in tickers:

            # source 1 data
            input = (ticker, sampling, key1)
            CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}' \
                      '&interval={}min&slice=year1month1&apikey={}'.format(*input)
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            # process df, calc rolling mean and std
            df = pd.DataFrame(columns=my_list[0], data=my_list[1:])[['time', 'close']]
            if df.empty:
                raise Exception("some tickers given are invalid")
            # source 2
            t_latest = dt.strptime(df['time'][0], '%Y-%m-%d %H:%M:%S')
            finnhub_client = finnhub.Client(api_key=key2)
            s2 = finnhub_client.quote(ticker)
            t_2 = dt.fromtimestamp(s2['t'])
            p_2 = s2['c']
            if t_2 > t_latest:
                t_2_str = dt.strftime(t_2, '%Y-%m-%d %H:%M:%S')
                new_row = {'time': t_2_str, 'close': p_2}
                df = df.append(new_row, ignore_index=True)
            # source 1 and 2 have been integrated
            df.set_index('time', inplace=True)
            df.sort_index(inplace=True)
            k_rolling = int(24 * 60 / sampling)
            # assume the 24 hours rolling window only applies to normal trading hours
            df['rolling_mean'] = df['close'].rolling(k_rolling).mean()
            df['rolling_std'] = df['close'].rolling(k_rolling).std()
            df.dropna(inplace=True)
            df['close'] = df['close'].astype(float)
            # implement momentum strategy
            df_signal = df.apply(signal, axis = 1)[0:-1]
            df_signal.fillna(method='ffill', inplace=True)
            df = df.iloc[1:]
            df_signal.index = df.index
            df['signal'] = df_signal
            df.dropna(inplace=True)
            # calc pnl
            df_diff = df['close'].diff()[1:]
            df_signal_m = df['signal'][0:-1]
            df_signal_m.index = df_diff.index
            df = df.iloc[1:]
            df['pnl'] = df_diff * df_signal_m
            # clean up the df
            df.insert(0, "ticker", ticker)
            df = df.rename(columns={'close': 'price'})
            df.drop(columns=['rolling_mean', 'rolling_std'], inplace=True)
            # save df to dict
            tickers_dict[f'{ticker}'] = df

    # pd.set_option('display.max_rows', None)
    output = pd.concat(tickers_dict.values(), sort=False).sort_index()
    # format
    output['signal'] = output['signal'].astype('int')
    output = output.round(2)
    output.reset_index(inplace=True)
    output = output.rename(columns={'time': 'datetime'})
    output['datetime'] = pd.to_datetime(output['datetime']).dt.strftime('%Y-%m-%d-%H:%M')
    # write to disk
    if save:
        filepath = os.path.join(os.getcwd(), "report.csv")
        print("report saved at this address: ")
        print(filepath)
        print()
        output.to_csv(filepath, index=False)

    return output


def main():
    args = parser()
    # task(host, args.port, args.tickers, args.sampling)
    print()
    print('inputs received for the server:')
    print('port: {}'.format(args.port))
    print('tickers: {}'.format(args.tickers))
    print('sampling period: {} min'.format(args.sampling))
    print()
    t = threading.Thread(target=task, args=(args.port, args.tickers, args.sampling))
    t.daemon = True  # run at background for a long running process
    t.start()

    while True:
        try:
            time.sleep(0.01)
        except KeyboardInterrupt:
            sys.exit(0)

if __name__ == "__main__":
    lock = threading.Lock()
    main()
