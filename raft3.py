#!/usr/bin/env python3
"""Raft consensus — leader election + log replication.

One file. Zero deps. Does one thing well.

Full Raft implementation: leader election with randomized timeouts,
log replication with majority commit, and term-based safety.
"""
import random, sys
from enum import Enum
from collections import defaultdict

class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    __slots__ = ('term', 'command')
    def __init__(self, term, command):
        self.term = term
        self.command = command
    def __repr__(self):
        return f"({self.term}:{self.command})"

class RaftNode:
    def __init__(self, node_id, peers):
        self.id = node_id
        self.peers = peers
        self.role = Role.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.leader_id = None
        # Leader state
        self.next_index = {}
        self.match_index = {}
        # Timing
        self.election_timeout = random.randint(150, 300)
        self.ticks_since_heartbeat = 0

    @property
    def quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def tick(self, cluster):
        self.ticks_since_heartbeat += 1
        if self.role == Role.LEADER:
            self._send_heartbeats(cluster)
            self.ticks_since_heartbeat = 0
        elif self.ticks_since_heartbeat >= self.election_timeout:
            self._start_election(cluster)

    def _start_election(self, cluster):
        self.term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self.ticks_since_heartbeat = 0
        self.election_timeout = random.randint(150, 300)
        votes = 1  # Vote for self
        last_log_term = self.log[-1].term if self.log else 0
        last_log_idx = len(self.log) - 1
        for pid in self.peers:
            peer = cluster.get(pid)
            if peer and peer._handle_vote_request(self.id, self.term, last_log_idx, last_log_term):
                votes += 1
        if votes >= self.quorum:
            self._become_leader(cluster)

    def _become_leader(self, cluster):
        self.role = Role.LEADER
        self.leader_id = self.id
        for pid in self.peers:
            self.next_index[pid] = len(self.log)
            self.match_index[pid] = -1
        # Announce leadership
        self._send_heartbeats(cluster)

    def _handle_vote_request(self, candidate_id, term, last_log_idx, last_log_term):
        if term < self.term:
            return False
        if term > self.term:
            self.term = term
            self.role = Role.FOLLOWER
            self.voted_for = None
        if self.voted_for is not None and self.voted_for != candidate_id:
            return False
        # Log up-to-date check
        my_last_term = self.log[-1].term if self.log else 0
        my_last_idx = len(self.log) - 1
        if last_log_term > my_last_term or (last_log_term == my_last_term and last_log_idx >= my_last_idx):
            self.voted_for = candidate_id
            self.ticks_since_heartbeat = 0
            return True
        return False

    def _send_heartbeats(self, cluster):
        for pid in self.peers:
            peer = cluster.get(pid)
            if not peer:
                continue
            ni = self.next_index.get(pid, len(self.log))
            prev_idx = ni - 1
            prev_term = self.log[prev_idx].term if prev_idx >= 0 and prev_idx < len(self.log) else 0
            entries = self.log[ni:]
            success = peer._handle_append(self.id, self.term, prev_idx, prev_term, entries, self.commit_index)
            if success:
                self.next_index[pid] = len(self.log)
                self.match_index[pid] = len(self.log) - 1
                self._advance_commit()
            elif ni > 0:
                self.next_index[pid] = max(0, ni - 1)

    def _handle_append(self, leader_id, term, prev_idx, prev_term, entries, leader_commit):
        if term < self.term:
            return False
        self.term = term
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self.voted_for = None
        self.ticks_since_heartbeat = 0
        # Log consistency check
        if prev_idx >= 0:
            if prev_idx >= len(self.log):
                return False
            if self.log[prev_idx].term != prev_term:
                self.log = self.log[:prev_idx]
                return False
        # Append entries
        idx = prev_idx + 1
        for e in entries:
            if idx < len(self.log):
                if self.log[idx].term != e.term:
                    self.log = self.log[:idx]
                    self.log.append(e)
                # else: already have it
            else:
                self.log.append(e)
            idx += 1
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
        return True

    def _advance_commit(self):
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term != self.term:
                continue
            repl_count = 1  # self
            for pid in self.peers:
                if self.match_index.get(pid, -1) >= n:
                    repl_count += 1
            if repl_count >= self.quorum:
                self.commit_index = n
                break

    def client_request(self, command, cluster):
        if self.role != Role.LEADER:
            return False
        self.log.append(LogEntry(self.term, command))
        self._send_heartbeats(cluster)
        return True

def main():
    random.seed(42)
    ids = ["n1", "n2", "n3", "n4", "n5"]
    cluster = {}
    for nid in ids:
        peers = [p for p in ids if p != nid]
        cluster[nid] = RaftNode(nid, peers)

    # Run election
    print("=== Raft Consensus ===\n")
    for _ in range(500):
        for node in cluster.values():
            node.tick(cluster)
    leaders = [n for n in cluster.values() if n.role == Role.LEADER]
    print(f"Leaders: {[l.id for l in leaders]} (term {leaders[0].term if leaders else '?'})")
    assert len(leaders) == 1, "Expected exactly one leader"

    leader = leaders[0]
    # Client requests
    for i in range(5):
        leader.client_request(f"SET x={i}", cluster)
    # Replicate
    for _ in range(10):
        for node in cluster.values():
            node.tick(cluster)

    print(f"\nLog replication:")
    for nid in sorted(cluster):
        n = cluster[nid]
        print(f"  {nid} ({n.role.value:9s}): log={n.log}, commit={n.commit_index}")

    # Verify all committed entries match
    committed = {nid: [e.command for e in cluster[nid].log[:cluster[nid].commit_index+1]] for nid in ids}
    vals = list(committed.values())
    assert all(v == vals[0] for v in vals), "Logs diverged!"
    print(f"\n✓ All nodes agree on committed log: {vals[0]}")

if __name__ == "__main__":
    main()
