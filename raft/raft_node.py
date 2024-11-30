import logging
import threading
import time
import random
from db.sqlite_manager import SQLiteManager

class RaftNode:
    def __init__(self, node_id, db_path):
        self.node_id = node_id
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.election_timeout = random.uniform(150, 300) / 1000
        self.lock = threading.Lock()
        self.sqlite_manager = SQLiteManager(db_path)

    def start(self):
        logging.info(f"Starting Raft node {self.node_id}")
        threading.Thread(target=self.run).start()

    def run(self):
        while True:
            if self.state == "follower":
                self.follower_state()
            elif self.state == "candidate":
                self.candidate_state()
            elif self.state == "leader":
                self.leader_state()

    def follower_state(self):
        logging.info(f"Node {self.node_id} is in follower state")
        start_time = time.time()
        while self.state == "follower":
            if time.time() - start_time > self.election_timeout:
                logging.info(f"Node {self.node_id} election timeout, starting leader election")
                self.state = "candidate"
            time.sleep(0.1)

    def candidate_state(self):
        logging.info(f"Node {self.node_id} is in candidate state")
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        # Simulate requesting votes from other nodes
        for _ in range(2):  # Assuming 3 nodes in total
            if random.choice([True, False]):
                votes += 1
        if votes > 1:
            self.state = "leader"
        else:
            self.state = "follower"

    def leader_state(self):
        logging.info(f"Node {self.node_id} is in leader state")
        while self.state == "leader":
            self.send_heartbeats()
            time.sleep(2)

    def send_heartbeats(self):
        logging.info(f"Node {self.node_id} is sending heartbeats")

    def handle_leader_election(self):
        logging.info(f"Node {self.node_id} is starting leader election")
        self.state = "candidate"

    def replicate_log_entries(self, entries):
        logging.info(f"Node {self.node_id} is replicating log entries")
        with self.lock:
            self.log.extend(entries)

    def apply_committed_entries(self, sqlite_manager, table_name):
        logging.info(f"Node {self.node_id} is applying committed entries to the state machine")
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                sqlite_manager.insert_record(entry, table_name)
                logging.info(f"Applied entry {entry} to the state machine")