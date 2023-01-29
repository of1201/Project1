import socket
import sys
import argparse

def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=str, default='127.0.0.1:8000')
    args = parser.parse_args()
    s = args.server
    server = s.split(':')
    return server

def main():
    server = parser()
    #print(args)

    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    try:
        host = server[0]
        port = int(server[1])
    except:
        print("invalid server argument")
        exit()
    server_address = (host, port)
    print(server)
    #print('connecting to {} port {}'.format(*server_address))
    try:
        sock.connect(server_address)
        while True:
            message = input()
            #send
            sock.sendall(message.encode('utf-8'))
            #receive
            data = sock.recv(32)
            #怎样才算server not responding啊......！
            print('received {!r}'.format(data))
    except Exception as e:
        print(e)

if __name__ == "__main__":
    try:
        main()
    except:
        exit()

