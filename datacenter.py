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

		self.leaderID = 0

		# need to put it into file later on
		self.votedFor = -1
		self.currentTerm = 0
		self.log = []
		self.poolsize = 100

		self.addressbook = {1:50001, 2:50002, 3:50003}

		self.peers = address.keys()
		self.peers.remove(self.id)
		self.majority = (len(self.peers) + 1)/2 + 1

		self.request_votes = self.peers[:]

		self.numVotes = 0

		self.lastLogIndex = 0
		self.lastLogTerm = 0

		self.prevLogTerm = 0
		self.prevLogIndex = 0


		self.listener = KThread(target = self.listen, args= (acceptor,))
		self.listener.start()



	def listen(self, on_accept):
		srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		srv.bind(("", self.port))
		print 'start listenning'
		while True:
			data, addr = srv.recvfrom(1024)
			thr = KThread(target=on_accept, args=(self, data, addr))
			thr.start()
		srv.close()

	def follower(self):
		print 'Running as a follower'
		self.role = 'follower'
		self.last_update = time.time()
		election_timeout = 5 * random.random() + 5
		while time.time() - self.last_update <= election_timeout:
			pass
		self.start_election()

		while True:
			self.last_update = time.time()
			election_timeout = 5 * random.random() + 5
			while time.time() - self.last_update <= election_timeout:
				pass
			if self.election.is_alive():
				self.election.kill()
			self.start_election()

	def start_election(self):
		self.role = 'candidate'
		self.election = KThread(target =self.thread_election,args = ())
		self.currentTerm += 1
		self.votedFor = self.id
		self.numVotes = 1
		self.election.start()

	def thread_election(self):
		print 'timouts, start a new election with term %d' % self.currentTerm
		self.role = 'candidate'
		self.request_votes = self.peers[:]
		sender = self.id

		while 1:
			print 'Send vote request to ', self.request_votes
			for peer in self.peers:
	 			if peer in self.request_votes:
	 				Msg = str(self.lastLogTerm) + ' ' + str(self.lastLogIndex)
	 				msg = RequestVoteMsg(sender, peer, self.currentTerm, Msg)
	 				data = pickle.dumps(msg)
	 				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	 				sock.sendto(data, ("", self.addressbook[peer]))
 			time.sleep(2) # wait for servers to receive

 	def leader(self):
 		print 'Running as a leader'
 		self.role = 'leader'
 		self.nextIndex = {}
 		self.matchIndex = {}
 		for peer in self.peers:
 			self.nextIndex[peer] = len(self.log) + 1
 			self.matchIndex[peer] = 0
		self.append_entries()

	def append_entries(self):
		while 1:
			for peer in self.peers:
				if len(self.log) >= self.nextIndex[peer]:
					entries = [self.log[self.nextIndex[peer]-1]]
				else:
					entries = []
				Msg = AppendEntriesMsg(self.id, peer, self.currentTerm, entries, self.commitIndex, self.prevLogIndex, self.prevLogTerm)
				data = pickle.dumps(Msg)
				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
				sock.sendto(data, ("", self.addressbook[peer]))
			time.sleep(2)

	def step_down(self):
		if self.role == 'candidate':
			print 'candidate step down when higher term'
			self.election.kill()
			self.last_update = time.time()
			self.role = 'follower'
		elif self.role == 'leader':
			self.leader_state.kill()
			self.follower_state = KThread(target = self.follower, args = ())
			self.follower_state.start()

	def run(self):
		time.sleep(3)
		self.follower_state = KThread(target = self.follower, args = ())
		self.follower_state.start()
		# while self.role != 'leader':
		# 	pass
		# self.follower_state.kill()
		# self.listener.kill()

		# print 'Now I am the leader for term %d' % self.currentTerm
		# self.leader_state.start()
			


