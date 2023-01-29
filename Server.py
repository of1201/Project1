import socket
import time
import subprocess
import argparse
import os.path
import threading
import re
from datetime import datetime
import requests  # v2.28.2
import sys


#我的第一个网页的apikey 是 DY66JSPKUIZMAEZ4,如果需要自己搞一个的话去这里https://www.alphavantage.co/support/#api-key
apikey_alphavantage = 'DY66JSPKUIZMAEZ4'
host = '192.168.2.32'#这个我不确定用这个还是localhost,等你们醒了可以试试用两个电脑..

def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', type=list, default=['AAPL','MSFT','TOST'])
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--sampling', type=int, default=5, choices=[5, 15, 30, 60]) 
    args = parser.parse_args()
    return args

def task(host, port, tickers, sampling):
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the port
    server_address = (host, port)
    print('starting up on {} port {}'.format(*server_address))
    sock.bind(('', port))

    # Listen for incoming connections
    sock.listen(1)

    #getting stock data and generating csv file
    generate(tickers, sampling)

    while True:
        # Wait for a connection
        print('waiting for a connection')
        connection, client_address = sock.accept()
        try:
            print('client connected:', client_address)
            while True:
                bdata = connection.recv(64)
                #print('received {!r}'.format(bdata))
                data = ((str(bdata)).split('\''))[1]
                #print(data)

                #不知道为什么如果不send回client信息的话client ctrl+c就会死循环..所以就算没要求return的report我也增加了一个send
                if re.match(r"^data \d\d\d\d-\d\d-\d\d-\d\d:\d\d$",data):
                    timestr = ticker = (data.split(' '))[1]
                    try:
                        time = datetime.strptime(timestr, '%Y-%m-%d-%H:%M')
                    except Exception as e:
                        connection.sendall('cannot convert input to date'.encode('utf-8'))

                    cur_time = datetime.now()
                    if time>cur_time:#specifying time in the future, returns the latest data
                        dataquery(cur_time)
                    else:
                        dataquery(time)
                    #print(time)
                    connection.sendall(b'0')
                    #print('data have date')
                elif re.match(r"^data$",data):#If time is not specified, the client returns latest data
                    time = datetime.now()
                    #print(time)
                    dataquery(time)
                    connection.sendall(b'0')
                    #print('data no date') 

                elif re.match(r"^delete TICKER .",data):
                    ticker = (data.split(' '))[2]
                    #print('delete TICKER', ticker)
                    try:
                        if ticker in tickers :
                            tickers.remove(ticker)
                            connection.sendall(b'0')
                        else :
                            connection.sendall(b'2')
                    except:#我实在不知道为什么这样一个请求能引发server error...他只是要删除一个数据而已..
                        connection.sendall(b'1')
                    #print(tickers)
                    

                elif re.match(r"^add TICKER .",data):
                    ticker = (data.split(' '))[2]
                    #print('add TICKER', ticker)
                    if ticker in tickers :
                        connection.sendall(b'0') #success, already exist我不确定这算success还是invalid
                    else:
                        msg=0
                        '''发送请求download historical data,失败的话根据失败信息（1,2）
                        code:


                        '''
                        if msg == 0: 
                            tickers.append(ticker)
                            connection.sendall(b'0')
                        elif meg == 1:#1=server error
                            connection.sendall(b'1') 
                        elif meg == 2:#2=ticker not found
                            connection.sendall(b'2') 
                    #print(tickers)

                elif data == 'report':
                    generate(tickers, sampling)
                    connection.sendall(b'report generated')
                    #print('report')

                else:
                    connection.sendall(b'unrecognized input from client')
                    print('unrecognized input '+data+' from client')
        except Exception as e:
            print(e)
        finally:
            connection.close()


def dataquery(time):
    '''
    query the server for latest price and signal available as of the time specified. 
    returns data like:
    AAPL    162.45,1
    MSFT    302.66,0

    '''

def generate(tickers, sampling):
    '''
    connects to Source 1 and constructs a series of stock prices sampled at X-minute intervals, using all available historical data for the given tickers.

    then immediately computes a Boolean trading signal series for the entire price time series, and does profit & loss calculation. Then this information is written to a CSV file named report.csv in the format below.
    E.g. (values not indicative of correct result)
    datetime, ticker, price, signal, pnl
    2022-04-25-11:00, AAPL, 421.04, -1, -0.02
    2022-04-25-11:00, MSFT, 132.95, 1, 0.06
    2022-04-25-11:05, AAPL, 421.09, -1, -0.05
    2022-04-25-11:05, MSFT, 132.98, 1, 0.03

    #print(data)
    '''



    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'
    r = requests.get(url)
    data = r.json()

    # print(data)




def main():
    args = parser()
    t = threading.Thread(target = task, args = (host, args.port, args.tickers, args.sampling))
    t.daemon= True
    t.start()

    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            exit(0)

if __name__ == "__main__":
    main()
