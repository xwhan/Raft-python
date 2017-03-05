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


class Server(object):
	def __init__(self, id_):
		self.id = id_
		self.config_file = 'config-%d' % self.id

		#self.load()
		self.role = 'follower'
		self.commitIndex = 0
		self.lastApplied = 0

		self.leaderID = 0

		address = json.load(file('config.json'))
		port_list = address['AddressBook']
		running = address['running']
		self.initial_state = address['initial_state']
		self.addressbook = {}
		for id_ in running:
			self.addressbook[id_] = port_list[id_ - 1]

		# need to put it into file later on
		self.load()

		self.port = self.addressbook[self.id]

		self.peers = self.addressbook.keys()
		self.peers.remove(self.id)
		self.majority = (len(self.peers) + 1)/2 + 1

		self.nextIndex = {}
 		self.matchIndex = {}
 		for peer in self.peers:
 			self.nextIndex[peer] = len(self.log) + 1
 			self.matchIndex[peer] = 0

		self.request_votes = self.peers[:]

		self.numVotes = 0

		self.lastLogIndex = 0
		self.lastLogTerm = 0


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
		self.save()
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
		self.append_entries()

	def append_entries(self):
		while 1:
			for peer in self.peers:
				if len(self.log) >= self.nextIndex[peer]:
					prevLogIndex = self.nextIndex[peer] - 1
					if prevLogIndex != 0:
						prevLogTerm = self.log[prevLogIndex-1].term
					else:
						prevLogTerm = 0
					entries = [self.log[self.nextIndex[peer]-1]]
				else:
					entries = []
					prevLogIndex = len(self.log)
					if prevLogIndex != 0:
						prevLogTerm = self.log[prevLogIndex-1].term
					else:
						prevLogTerm = 0

				Msg = AppendEntriesMsg(self.id, peer, self.currentTerm, entries, self.commitIndex, prevLogIndex, prevLogTerm)
				data = pickle.dumps(Msg)
				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
				sock.sendto(data, ("", self.addressbook[peer]))
			time.sleep(1)

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

	def load(self):
		try:
			with open(self.config_file) as f:
				serverConfig = pickle.load(f)
		except Exception as e:
			serverConfig = ServerConfig(100, 0, -1, [])

		self.poolsize = serverConfig.poolsize
		self.currentTerm = serverConfig.currentTerm
		self.votedFor = serverConfig.votedFor
		self.log = serverConfig.log

	def save(self):
		serverConfig = ServerConfig(self.poolsize, self.currentTerm, self.votedFor, self.log)
		with open(self.config_file, 'w') as f:
			pickle.dump(serverConfig, f)

	def run(self):
		time.sleep(1)
		self.follower_state = KThread(target = self.follower, args = ())
		self.follower_state.start()
		# while self.role != 'leader':
		# 	pass
		# self.follower_state.kill()
		# self.listener.kill()

		# print 'Now I am the leader for term %d' % self.currentTerm
		# self.leader_state.start()
			


