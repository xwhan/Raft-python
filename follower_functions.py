import socket
import pickle
import time
import random

from messages import *
from KThread import *

def acceptor(server, data, addr):
	Msg = pickle.loads(data)
	_type = Msg.type

	# deal with client's message
	if _type == 'client' or _type == 'redirect':

		if _type == 'redirect':
			addr = Msg.addr

		msg_string = Msg.request_msg
		if msg_string == 'show':
			state = server.poolsize
			committed_log = ''
			for idx in range(0,server.commitIndex):
				entry = server.log[idx]
				committed_log += str(entry.command) + ' '

			all_log = ''
			for entry in server.log:
				all_log += str(entry.command) + ' '

			show_msg = 'state machine: ' + str(state) + '\n' + 'committed log:' + committed_log + '\n' + 'all log:' + all_log
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			s.sendto(show_msg, addr)
			s.close()
		else:
			ticket_num = int(msg_string.split()[1])
			if server.role == 'leader':
				print "I am the leader, customer wants to buy %d tickets" % ticket_num

				# check whether this command has already been 
				for idx, entry in enumerate(server.log):
					if entry.uuid == Msg.uuid:
						if server.commitIndex >= idx + 1:
							s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
							s.sendto('Your request has been fullfilled', addr)
							s.close()
						else: # ignore
							pass
						return # ignore this new command

				newEntry = LogEntry(server.currentTerm, ticket_num, addr, Msg.uuid)
				s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				s.sendto('The Leader gets your request', addr)
				s.close()
				server.log.append(newEntry)
				server.save()
			# we need to redirect the request to leader
			else:
				print 'redirect the request to leader'
				if server.leaderID != 0:
					redirect_target = server.leaderID
				else:
					redirect_target = random.choice(server.peers)
				redirect_msg = RequestRedirect(msg_string, Msg.uuid, addr)
				s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				s.sendto(pickle.dumps(redirect_msg), ("",server.addressbook[redirect_target]))
				s.close()
		return

	_sender = Msg.sender
	_term = Msg.term
	

	if _type == 1: # requestvote message
		_msg = Msg.data
		print '---------Get requestvote message---------'
		_msg = _msg.split()
		log_info = (int(_msg[0]), int(_msg[1]))
		if _term < server.currentTerm:
			print 'rejected due to old term'
			voteGranted = 0		
		elif _term == server.currentTerm:
			if log_info >= (server.lastLogTerm, server.lastLogIndex) and (server.votedFor == -1 or server.votedFor == _sender):
				voteGranted = 1
				server.votedFor = _sender
				server.save()
			else:
				voteGranted = 0
		else:
			# find higher term in RequestVoteMsg
			server.currentTerm = _term
			server.save()
			server.step_down()

			if log_info >= (server.lastLogTerm, server.lastLogIndex):
				voteGranted = 1
				server.votedFor = _sender
				server.save()
			else:
				voteGranted = 0
		reply = str(voteGranted)
		reply_msg = VoteResponseMsg(server.id, _sender, server.currentTerm, reply)
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.sendto(pickle.dumps(reply_msg), ("",server.addressbook[_sender]))

	elif _type == 2: # Vote response message
		_msg = Msg.data
		print '---------Get vote response message---------'
		voteGranted = int(_msg)
		if voteGranted:
			if server.role == 'candidate':
				server.request_votes.remove(_sender)
				server.numVotes += 1
				if server.numVotes >= server.majority:
					print 'Get majority votes, become leader at Term %d' % server.currentTerm
					if server.election.is_alive():
						server.election.kill()
				# becomes a leader
					server.role = 'leader'
					server.follower_state.kill()
					server.leader_state = KThread(target = server.leader, args = ())
					server.leader_state.start()

		else:
			if _term > server.currentTerm: # discover higher term
				server.currentTerm = _term
				server.save()
				if server.role == 'candidate':
					server.step_down()

			print 'vote rejected by %d' % _sender

	elif _type == 0: # AppendEntries msg
		# print '---------Get AppendEntries message---------'
		entries = Msg.entries
		leaderCommit = Msg.commitIndex
		prevLogTerm = Msg.prevLogTerm
		prevLogIndex = Msg.prevLogIndex

		matchIndex = server.commitIndex

		# This is a valid new leader
		if _term >= server.currentTerm:
			server.currentTerm = _term
			server.save()
			server.step_down()
			if server.role == 'follower':
				server.last_update = time.time()
			if prevLogIndex != 0:
				if len(server.log) >= prevLogIndex:
					if server.log[prevLogIndex - 1].term == prevLogTerm:
						success = 'True'
						server.leaderID = _sender
						server.log = server.log[:prevLogIndex] + entries
						if len(entries) != 0:
							matchIndex = len(server.log)
						server.save()
					else:
						success = 'False'
				else:
					success = 'False'
			else:
				success = 'True'
				if len(entries) != 0:
					server.log = server.log[:prevLogIndex] + entries
					server.save()
					matchIndex = len(server.log)
				server.leaderID = _sender
		else:
			success = 'False'

		if leaderCommit > server.commitIndex:
			lastApplied = server.commitIndex
			server.commitIndex = min(leaderCommit, len(server.log))
			if server.commitIndex > lastApplied:
				server.poolsize = server.initial_state
				for idx in range(1, server.commitIndex + 1):
					server.poolsize -= server.log[idx - 1].command


		reply_msg = AppendEntriesResponseMsg(server.id, _sender, server.currentTerm, success, matchIndex)
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.sendto(pickle.dumps(reply_msg), ("",server.addressbook[_sender]))

	elif _type == 3: # AppendEntriesResponse:
		#print '---------Get AppendEntries Response message---------'
		success = Msg.success
		matchIndex = Msg.matchIndex

		if success == 'False':
			if _term > server.currentTerm:
				server.currentTerm = _term
				server.save()
				server.step_down()
			else:
				server.nextIndex[_sender] -= 1
		else:
			if server.nextIndex[_sender] <= len(server.log) and matchIndex > server.matchIndex[_sender]:
				server.matchIndex[_sender] = matchIndex
				server.nextIndex[_sender] += 1

			if server.commitIndex < max(server.matchIndex.values()):
				start = server.commitIndex + 1
				for N in range(start,max(server.matchIndex.values()) + 1):
					compare = [item >= N for item in server.matchIndex.values()]
					if sum(compare) + 1 == server.majority and server.log[N-1].term == server.currentTerm:
						for idx in range(server.commitIndex + 1, N + 1):
							server.poolsize -= server.log[idx-1].command
							server.save()
							s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
							s.sendto('Your request is fullfilled',server.log[idx-1].addr)
							s.close()
						server.commitIndex = N





