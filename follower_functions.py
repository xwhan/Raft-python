import socket
import pickle
import time
import random

from messages import *
from KThread import *

def acceptor(server, data, addr):
	Msg = pickle.loads(data)
	_type = Msg.type

	# deal with config changes
	if _type == 'change':
		if Msg.phase == 1:
			print 'Config change phase 1'
			server.during_change = 1
			server.new = Msg.new_config
			server.old = server.peers[:]
			server.old.append(server.id)
			if Msg.addr != None:
				addr = Msg.addr
			newEntry = LogEntry(server.currentTerm, Msg, addr, Msg.uuid, 1)
			server.log.append(newEntry)
			server.peers = list(set(server.old + server.new))
			server.peers.remove(server.id)
			server.save()
			print 'Config change phase 1 applied'
			#return
		else:
			print 'Config change phase 2'
			server.during_change = 2
			server.new = Msg.new_config
			if Msg.addr != None:
				addr = Msg.addr
			newEntry = LogEntry(server.currentTerm, Msg, addr, Msg.uuid, 2)
			server.log.append(newEntry)
			server.peers = server.new[:]
			if server.id in server.peers:
				server.peers.remove(server.id)		
			server.save()
			print 'Config change phase 2 applied, running peers'
			#return

		# if server.role == 'leader':
		# 	for peer in server.peers:
		# 		if peer not in server.nextIndex:
		# 			server.nextIndex[peer] = len(server.log) + 1 
		# 			server.matchIndex[peer] = 0

		if server.role != 'leader':
			print 'redirect config change to the leader'
			if server.leaderID != 0:
				redirect_target = server.leaderID
			else:
				redirect_target = random.choice(server.peers)
			if Msg.addr != None:
				addr = Msg.addr
			redirect_msg = ConfigChange(Msg.new_config, Msg.uuid, Msg.phase, addr)
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			s.sendto(pickle.dumps(redirect_msg), ("",server.addressbook[redirect_target]))
			s.close()			

		#server.log.append(newEntry)
		#server.save()
		return

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
				if entry.type == 0:
					committed_log += str(entry.command) + ' '
				elif entry.type == 1:
					committed_log += 'new_old' + ' '
				else:
					committed_log += 'new' + ' '

			all_log = ''
			for entry in server.log:
				if entry.type == 0:
					all_log += str(entry.command) + ' '
				elif entry.type == 1:
					all_log += 'new_old' + ' '
				else:
					all_log += 'new' + ' '

			show_msg = 'state machine: ' + str(state) + '\n' + 'committed log: ' + committed_log + '\n' + 'all log:' + all_log + '\n' + 'status: ' + str(server.during_change)
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			s.sendto(show_msg, addr)
			s.close()

		else:
			ticket_num = int(msg_string.split()[1])
			if server.role == 'leader':
				print "I am the leader, customer wants to buy %d tickets" % ticket_num

				if ticket_num > server.poolsize:
					print 'Tickets not enough'
					s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
					s.sendto('We do not have enough tickets', addr)
					s.close()
					return

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
		if _sender not in server.peers:
			return
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
			if server.during_change == 0:
				if server.role == 'candidate':
					server.request_votes.remove(_sender)
					server.numVotes += 1
					if server.numVotes == server.majority:
						print 'Get majority votes, become leader at Term %d' % server.currentTerm
						if server.election.is_alive():
							server.election.kill()
					# becomes a leader
						server.role = 'leader'
						server.follower_state.kill()
						server.leader_state = KThread(target = server.leader, args = ())
						server.leader_state.start()
			elif server.during_change == 1:
				server.request_votes.remove(_sender)
				if _sender in server.old:
					server.oldVotes += 1 
				if _sender in server.new:
					server.newVotes += 1
				majority_1 = len(server.old)/2 + 1
				majority_2 = len(server.new)/2 + 1
				if server.oldVotes >= majority_1 and server.newVotes >= majority_2:
					print 'Get majority from both old and new, become leader at Term %d' % server.currentTerm
					if server.election.is_alive():
						server.election.kill()
					server.role = 'leader'
					server.follower_state.kill()
					server.leader_state = KThread(target = server.leader, args = ())
					server.leader_state.start()
			else:
				server.request_votes.remove(_sender)
				if _sender in server.peers:
					server.newVotes += 1
				majority = len(server.new)/2 + 1
				if server.newVotes >= majority:
					print 'Get majority from new, become leader at Term %d' % server.currentTerm
					if server.election.is_alive():
						server.election.kill()
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
						if len(entries) != 0:
							server.log = server.log[:prevLogIndex] + entries
							matchIndex = len(server.log)
							if entries[0].type == 1:
								server.during_change = 1
								server.new = entries[0].command.new_config[:]
								server.old = server.peers[:]
								server.old.append(server.id)
								server.peers = list(set(server.old + server.new))
								server.peers.remove(server.id)			
							elif entries[0].type == 2:
								server.during_change = 2
								server.new = entries[0].command.new_config[:]
								server.peers = server.new[:]
								server.peers.remove(server.id)	
								print 'follower applied new config, running peers', server.peers
								server.save()
					else:
						success = 'False'
				else:
					success = 'False'
			else:
				success = 'True'
				if len(entries) != 0:
					server.log = server.log[:prevLogIndex] + entries
					if entries[0].type == 1:
						server.during_change = 1
						server.new = entries[0].command.new_config[:]
						server.old = server.peers[:]
						server.old.append(server.id)
						server.peers = list(set(server.old + server.new))
						server.peers.remove(server.id)			
					elif entries[0].type == 2:
						server.during_change = 2
						server.new = entries[0].command.new_config[:]
						server.peers = server.new[:]
						server.peers.remove(server.id)	
						print 'follower applied new config, running peers', server.peers

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
					if server.log[idx-1].type == 0:
						server.poolsize -= server.log[idx - 1].command
					elif server.log[idx-1].type == 2:
						server.during_change = 0

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
					if server.during_change == 0:
						# not in config change
						compare = 1
						for key, item in server.matchIndex.items():
							if key in server.peers and item >= N:
								compare += 1
						majority = (len(server.peers) + 1)/2 + 1
						if compare == server.majority and server.log[N-1].term == server.currentTerm:
							for idx in range(server.commitIndex + 1, N + 1):
								server.poolsize -= server.log[idx-1].command
								server.save()
								s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
								s.sendto('Your request is fullfilled',server.log[idx-1].addr)
								s.close()
								print 'reply once'
							server.commitIndex = N
					elif server.during_change == 1:
						majority_1 = len(server.old)/2 + 1
						majority_2 = len(server.new)/2 + 1
						votes_1 = 0
						votes_2 = 0
						if server.id in server.old:
							votes_1 = 1
						if server.id in server.new:
							votes_2 = 1
						for key, item in server.matchIndex.items():
							if item >= N:
								if key in server.old:
									votes_1 += 1
								if key in server.new:
									votes_2 += 1
						if votes_1 >= majority_1 and votes_2 >= majority_2 and server.log[N-1].term == server.currentTerm:
							server.commitIndex = N
							poolsize = server.initial_state
							for idx in range(1, N + 1):
								if server.log[idx-1].type == 0:
									poolsize -= server.log[idx-1].command
							server.poolsize = poolsize
							server.save()
							s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
							s.sendto('Your request is fullfilled',server.log[idx-1].addr)
							s.close()
							# print 'send old_new once'
											
					else:
						majority = len(server.new)/2 + 1
						votes = 0
						if server.id in server.new:
							votes = 1
						for key, item in server.matchIndex.items():
							if item >= N:
								if key in server.new:
									votes += 1
						if votes == majority and server.log[N-1].term == server.currentTerm:
							print '----------here 2----------'
							for idx in range(server.commitIndex + 1, N + 1):
								if server.log[idx-1].type == 0:
									server.poolsize -= server.log[idx-1].command
									server.save()
									s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
									s.sendto('Your request is fullfilled',server.log[idx-1].addr)
									s.close()
									server.commitIndex = idx
								elif server.log[idx-1].type == 2:
									server.commitIndex = idx
									time.sleep(1)
									if not server.id in server.new:
										print 'I am not in the new configuration'
										server.step_down()
									server.during_change = 0
									server.save()
									s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
									s.sendto('Your request is fullfilled',server.log[idx-1].addr)
									s.close()
								# print 'send new once'





