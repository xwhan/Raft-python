import time
import socket


class BaseMessage(object):
    AppendEntries = 0
    RequestVote = 1
    RequestVoteResponse = 2
    Response = 3

    def __init__(self, sender, receiver, term, data):
        self.sender = sender
        self.receiver = receiver
        self.data = data
        self.term = term


class RequestVoteMsg(BaseMessage):

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.RequestVote


class VoteResponseMsg(BaseMessage):

    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.RequestVoteResponse