import socket
import sys
import argparse
import json
import re

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
    #print('connecting to {} port {}'.format(*server_address))

    try:
        # connect to server
        sock.connect(server_address)
        print('Connected to: {}'.format(server))
        while True:
            message = input()
            # send message to server
            sock.sendall(message.encode('utf-8'))
            #receive
            if re.match(r"^data$", message) or re.match(r"^data \d\d\d\d-\d\d-\d\d-\d\d:\d\d$", message):
                data = json.loads(sock.recv(4096).decode())
                for i in data:
                    print(i)
            else:
                data = str(sock.recv(4096), 'utf-8')
                print(data)
    except Exception as e:
        print(e)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()

