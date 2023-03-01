import socket
import sys
import argparse
import json
import re

class communication:
    """
    get client input, send request to server and receive results from server
    """

    def __init__(self):
        self.server = None
        self.parser = argparse.ArgumentParser()
        self.args = None
        self.server_address = None
        self.sock = None

    def get_arguments(self):
        """
        fetch client arguments
        """

        self.parser.add_argument('--server', type=str, default='127.0.0.1:8000')
        self.args = self.parser.parse_args()
        self.server = self.args.server.split(':')

    def build_tcp(self):
        """
        connect with server and have interaction with server
        """

        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        try:
            host = self.server[0]
            port = int(self.server[1])
        except:
            print("invalid server argument")
            sys.exit(1)

        # connect with server
        try:
            self.server_address = (host, port)
            self.sock.connect(self.server_address)
            print('Connected to: {}'.format(self.server))
        except:
            print("failed to connect with server")
            sys.exit(1)

        self.tcp_interaction()

    def tcp_interaction(self):
        """
        interact with server
        """

        while True:  # client can continuously send inputs
            try:
                message = input()  # store client input

                # send message to server
                self.sock.sendall(message.encode('utf-8'))

                # receive
                # if client is querying for records from the report
                if re.match(r"^data$", message) or re.match(r"^data \d\d\d\d-\d\d-\d\d-\d\d:\d\d$", message):
                    result = json.loads(self.sock.recv(4096).decode())

                    for i in result:  # print result received from server
                        print(i)
                # for other client inputs
                else:
                    result = str(self.sock.recv(4096), 'utf-8')
                    print(result)

            except Exception as e:
                print(e)
                sys.exit(1)
            except KeyboardInterrupt:
                sys.exit(0)


if __name__ == "__main__":

    comm = communication()  # create communication object

    comm.get_arguments()  # parse client inputs

    comm.build_tcp()  # enable client to interact with server