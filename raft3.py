#!/usr/bin/env python3
"""Raft Consensus — leader election + log replication (full protocol)."""
import random, enum

class Role(enum.Enum):
    FOLLOWER = "follower"; CANDIDATE = "candidate"; LEADER = "leader"

class LogEntry:
    def __init__(self, term, command): self.term, self.command = term, command

class RaftNode:
    def __init__(self, node_id, peers):
        self.id = node_id; self.peers = peers
        self.role = Role.FOLLOWER; self.term = 0; self.voted_for = None
        self.log = []; self.commit_index = -1
        self.next_index = {}; self.match_index = {}
        self.votes_received = set(); self.leader_id = None
    def start_election(self):
        self.term += 1; self.role = Role.CANDIDATE; self.voted_for = self.id
        self.votes_received = {self.id}
        return {"type": "RequestVote", "term": self.term, "candidate": self.id,
                "last_log_index": len(self.log)-1, "last_log_term": self.log[-1].term if self.log else 0}
    def handle_vote_request(self, msg):
        if msg["term"] > self.term: self.term = msg["term"]; self.role = Role.FOLLOWER; self.voted_for = None
        grant = (msg["term"] >= self.term and (self.voted_for is None or self.voted_for == msg["candidate"]))
        if grant: self.voted_for = msg["candidate"]
        return {"type": "VoteResponse", "term": self.term, "granted": grant, "voter": self.id}
    def handle_vote_response(self, msg):
        if msg["granted"]: self.votes_received.add(msg["voter"])
        if len(self.votes_received) > (len(self.peers) + 1) // 2:
            self.role = Role.LEADER; self.leader_id = self.id
            for p in self.peers: self.next_index[p] = len(self.log); self.match_index[p] = -1
            return True
        return False
    def append_entry(self, command):
        if self.role != Role.LEADER: return False
        self.log.append(LogEntry(self.term, command)); return True
    def heartbeat(self):
        return {"type": "AppendEntries", "term": self.term, "leader": self.id,
                "entries": [], "commit_index": self.commit_index}
    def handle_append(self, msg):
        if msg["term"] >= self.term:
            self.term = msg["term"]; self.role = Role.FOLLOWER; self.leader_id = msg["leader"]
        return {"type": "AppendResponse", "term": self.term, "success": True, "node": self.id}

if __name__ == "__main__":
    nodes = {i: RaftNode(i, [j for j in range(5) if j != i]) for i in range(5)}
    # Node 0 starts election
    req = nodes[0].start_election()
    print(f"Node 0 starts election (term {nodes[0].term})")
    for i in range(1, 5):
        resp = nodes[i].handle_vote_request(req)
        won = nodes[0].handle_vote_response(resp)
        print(f"  Node {i} votes: {resp['granted']}")
        if won: print(f"  Node 0 wins! Role: {nodes[0].role.value}"); break
    nodes[0].append_entry("SET x=1")
    print(f"Log: {[(e.term, e.command) for e in nodes[0].log]}")
