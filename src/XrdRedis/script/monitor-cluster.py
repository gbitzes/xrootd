#!/usr/bin/env python3
import argparse
import redis
import time
import inspect

# Poor man's python 3.6 f-strings
# Uses local variables to format a string, ie
# b = 1, c = 2
# f("{b} {c}") => "1 2"
# When in 15 years python 3.6 becomes the minimum version to support,
# all invocations f("something") could be replaced with f"something"
def f(s):
    return s.format(**inspect.currentframe().f_back.f_locals)
def ff(s):
    return s.format(**inspect.currentframe().f_back.f_back.f_locals)

def log(s):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print("[{0}] {1}".format(timestamp, ff(s)))

def extract(resp, item):
    for index, entry in enumerate(resp):
        if entry == item:
            return resp[index+1]

def get_all_attrs(servers, attr):
    ret = []
    for server in servers:
        if server.state == "dead": continue

        ret.append(getattr(server, attr))
    return ret

def checkConsistency(servers):
    leader = None
    term = None
    clusterID = None
    participants = None

    for server in servers:
        if server.state == "dead":
            continue

        if not leader: leader = server.leader
        if not term: term = server.term
        if not clusterID: clusterID = server.clusterID
        if not participants: participants = server.participants

        if leader != server.leader:
            log("DISAGREEMENT on leader: " + str(get_all_attrs(servers, "leader")))
        if term != server.term:
            log("DISAGREEMENT on term: " + str(get_all_attrs(servers, "term")))
        if clusterID != server.clusterID:
            log("PANIC: disagreement on clusterID, " + str(get_all_attrs(servers, "clusterID")))
        if participants != server.participants:
            log("DISAGREEMENT on participants: {participants} vs {server.participants}")

class RaftServer:
    hostname = ""
    port = 0

    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port

        self.myself = hostname + ":" + port

        self.term = None
        self.leader = None
        self.state = None
        self.clusterID = None
        self.participants = None

    def termTransition(self, newterm):
        if self.term != newterm:
            log("Term transition for {self.myself}: {self.term} => {newterm}")
            if self.term and self.term > newterm: log("PANIC: term went backwards")
            self.term = newterm

    def leaderTransition(self, newleader):
        if self.leader != newleader:
            log("Leader transition for {self.myself}: {self.leader} => {newleader}")
            self.leader = newleader

    def stateTransition(self, newstate):
        if self.state != newstate:
            log("State transition for {self.myself}: {self.state} => {newstate}")
            self.state = newstate

    def participantsTransition(self, newparticipants):
        if self.participants != None and self.participants != newparticipants:
            log("State transition for {self.myself}: {self.participants} => {newparticipants}")
        self.participants = newparticipants


    def markDead(self):
        self.stateTransition("dead")

    def update(self, resp):
        for index, entry in enumerate(resp):
            if entry == "CLUSTER-ID":
                newclusterid = resp[index+1]

                if self.clusterID == None: self.clusterID = newclusterid
                if self.clusterID != newclusterid:
                    log("PANIC: clusterID of {myself} changed from {self.clusterID} to {newclusterid}")
                self.clusterID = newclusterid
            elif entry == "STATE":
                self.stateTransition(resp[index+1])
            elif entry == "LEADER":
                self.leaderTransition(int(resp[index+1]))
            elif entry == "TERM":
                self.termTransition(int(resp[index+1]))
            elif entry == "PARTICIPANTS":
                self.participantsTransition(resp[index+1])
            elif entry == "MYSELF":
                identity = resp[index+1]
                if self.myself != identity:
                    log("PANIC: self-reported identity of {myself} is incorrect: {identity}")
            elif entry == "MY-ID":
                identity = int(resp[index+1])
                if self.participants.split(",")[identity] != self.myself:
                    log("PANIC: self-reported id of {self.myself} is wrong (MY-ID = {identity}, participants = {self.participants})")

    def from_string(serialized):
        servers = serialized.split(",")
        ret = []
        for server in servers:
            sp = server.split(":")
            ret.append(RaftServer(sp[0], sp[1]))
        return ret

def getargs():
      parser = argparse.ArgumentParser(description='Monitor events on a raft cluster')
      parser.add_argument('--participants', required=True, help='The list of participants in the cluster')
      return parser.parse_args()

def monitor(servers):
    while True:
        for server in servers:
            try:
                conn = redis.StrictRedis(host=server.hostname, port=server.port)

                response = conn.execute_command("RAFT_INFO")
                unicode_response = [x.decode('utf-8') for x in response]
                server.update(unicode_response)
            except redis.exceptions.ConnectionError:
                server.markDead()

        checkConsistency(servers)
        time.sleep(1)

def main():
    args = getargs()
    print(args.participants)
    monitor(RaftServer.from_string(args.participants))

if __name__ == '__main__':
    main()
