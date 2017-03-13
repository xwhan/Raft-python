import socket
import json
import pickle
import uuid
import time

from KThread import *
from messages import *

class client(object):
    cnt = 0
    def __init__(self):
        client.cnt = client.cnt+1
        self.id = client.cnt
        self.num_of_reply = 0 

    def buyTickets(self, port, buy_msg, uuid):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request(buy_msg, uuid)
        s.sendto(pickle.dumps(msg), ("", port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    self.num_of_reply += 1
                    print(reply)
                if self.num_of_reply == 2:
                    break
            except Exception as e:
                print 'Connection refused'
 
        s.close()

    def show_state(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = Request('show')
        s.sendto(pickle.dumps(msg),("",port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print 'Pool Size', reply
                    break
            except Exception as e:
                print 'Connection refused'

    def config_change(self, port, new_config, uuid):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = ConfigChange(new_config, uuid, 1)
        s.sendto(pickle.dumps(msg),("",port))
        # we need to get committed twice
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    self.num_of_reply != 1
                    print reply
                    break
            except Exception as e:
                print 'Connection refused'
        msg = ConfigChange(new_config, uuid, 2)
        s.sendto(pickle.dumps(msg), ("",port))
        while 1:
            try:
                reply, addr = s.recvfrom(1024)
                if reply != '':
                    print reply
                    break
            except Exception as e:
                print 'Connection refused'
        s.close()

def main():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        ports = config['AddressBook']
        num_ports = len(ports)
    except Exception as e:
        raise e

    while True:
        customer = client()
        server_id = input('Which datacenter do you want to connect to? 1-%d: ' % num_ports )
        request = raw_input('How can we help you? --')
        if request == 'show':
            requestThread = KThread(target = customer.show_state, args = (ports[server_id - 1],))
            timeout = 5
        elif request.split()[0] == 'change':
            uuid_ = uuid.uuid1()
            msg_split = request.split()
            new_config_msg = msg_split[1:]
            new_config = [int(item) for item in new_config_msg]
            print new_config
            requestThread = KThread(target = customer.config_change, args = (ports[server_id - 1], new_config, uuid_))
            timeout = 20
        else:
            uuid_ = uuid.uuid1()
            requestThread = KThread(target = customer.buyTickets, args =  (ports[server_id - 1], request, uuid_))
            timeout = 5
        start_time = time.time()
        requestThread.start()
        while time.time() - start_time < timeout:
            if not requestThread.is_alive():
                break
        if requestThread.is_alive():
            print 'Timeout! Try again'
            requestThread.kill()

if __name__ == '__main__':
    main()