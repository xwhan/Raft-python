import sys
import time
import json
import threading
import socket
import thread
from multiprocessing import Process
import random
import pickle

from KThread import *
from messages import *
from follower_functions import *

address = {1:50001, 2:50002, 3:50003}

class Server(object):
	def __init__(self, id_):
		self.id = id_
		self.port = address[id_]
		#self.load()
		self.role = 'follower'
		self.commitIndex = 0
		self.lastApplied = 0

		self.a2c = {}

		# need to put it into file later on
		self.votedFor = -1
		self.currentTerm = 0
		self.log = []

		self.addressbook = {1:50001, 2:50002, 3:50003}

		self.peers = address.keys()
		self.peers.remove(self.id)
		self.majority = (len(self.peers) + 1)/2 + 1

		self.request_votes = self.peers[:]

		self.numVotes = 0


		self.nextIndex = [len(self.log) + 1]*len(self.peers)
		self.matchIndex = [0]*len(self.peers)

		self.lastLogIndex = 0
		self.lastLogTerm = 0

		self.follower_state = KThread(target = self.follower, args = ())


	def listen(self, follower_accept):
		srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		srv.bind(("", self.port))
		print 'start listenning'
		while True:
			data, addr = srv.recvfrom(1024)
			self.listener = KThread(target=follower_accept, args=(self, data, addr))
			self.listener.start()
		srv.close()

	def follower(self):
		print 'Running as a follower'
		self.listener = KThread(target = self.listen, args= (follower_acceptor,))
		self.listener.start()
		election_timeout = 0.5 * random.random() + 10
		time.sleep(election_timeout)
		while True:
			self.election = KThread(target =self.start_election,args = ())
			election_timeout = 0.5 * random.random() + 20
			self.currentTerm += 1
			self.votedFor = self.id
			self.numVotes = 1
			self.election.start()
			time.sleep(election_timeout)
			if self.election.is_alive():
				print 'Terminate election due to timeout'
				self.election.kill()

	def start_election(self):
		print 'timouts, start a new election with term %d' % self.currentTerm
		self.role = 'candidate'
		self.request_votes = self.peers[:]

		print 'Send vote request to ', self.request_votes

		sender = self.id

		while 1:
			for peer in self.peers:
	 			if peer in self.request_votes:
	 				Msg = str(self.lastLogTerm) + ' ' + str(self.lastLogIndex)
	 				msg = RequestVoteMsg(sender, peer, self.currentTerm, Msg)
	 				data = pickle.dumps(msg)
	 				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	 				sock.sendto(data, ("", self.addressbook[peer]))
 			time.sleep(6) # wait for servers to receive


	def run(self):
		time.sleep(3)
		self.follower_state.start()
		while self.role != 'leader':
			pass
		self.follower_state.kill()
		print 'Now I am the leader for term %d' % self.currentTerm
			


