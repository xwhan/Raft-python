import socket
import pickle

from messages import *

def follower_acceptor(server, data, addr):
	Msg = pickle.loads(data)
	_type = Msg.type
	_sender = Msg.sender
	_term = Msg.term
	_msg = Msg.data

	if _type == 1: # requestvote message
		print '---------Get requestvote message---------'
		_msg = _msg.split()
		log_info = (int(_msg[0]), int(_msg[1]))
		if _term < server.currentTerm:
			print 'rejected due to old term'
			voteGranted = 0		
		elif _term == server.currentTerm:
			if log_info >= (server.lastLogTerm, server.lastLogIndex) and (server.votedFor == -1 or server.votedFor == _sender):
				voteGranted = 1
			else:
				voteGranted = 0
		else:
			if log_info >= (server.lastLogTerm, server.lastLogIndex):
				voteGranted = 1
				server.currentTerm = _term
			else:
				voteGranted = 0
		reply = str(voteGranted)
		reply_msg = VoteResponseMsg(server.id, _sender, server.currentTerm, reply)
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.sendto(pickle.dumps(reply_msg), ("",server.addressbook[_sender]))

	elif _type == 2: # Vote response message
		print '---------Get vote response message---------'
		voteGranted = int(_msg)
		if voteGranted:
			print server.role
			if server.role == 'candidate':
				server.request_votes.remove(_sender)
			server.numVotes += 1
			if server.numVotes >= server.majority:
				print 'Get majority votes, become leader at Term %d' % server.currentTerm
				if server.election.is_alive():
					server.election.kill()
				server.role = 'leader'

		else:
			if _term > server.currentTerm:
				server.currentTerm = _term
			print 'vote rejected by %d' % _sender


