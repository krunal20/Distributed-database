import logging
import random
from db.sqlite_manager import SQLiteManager
from db.shard_manager import ShardManager
from raft.raft_node import RaftNode
from utils.config import Config, generate_unique_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DistributedDatabase:
    def __init__(self, db_path, node_id):
        self.nodes = {}
        self.add_node(node_id)
        self.shard_manager = ShardManager()
        self.max_shards_per_node = Config.MAX_SHARDS_PER_NODE
        self.new_nodes = []
        self.active_nodes = list(self.nodes.values())
        self.shards_in_current_group = 0
        self.shard_to_nodes = {}      # Map shard IDs to node IDs
        self.node_shard_count = {}    # Map node IDs to shard counts
        self.node_shard_count[node_id] = 0

    def insert_data(self, name, email, data):
        try:
            record_id = generate_unique_id()
            shard_id = self.shard_manager.determine_shard(record_id)
            if shard_id is None:
                # Need to create a new shard
                shard_id = len(self.shard_manager.shards)
                self.shard_manager.create_shard(shard_id)
                # Check if existing nodes have capacity
                available_nodes = [node for node_id, node in self.nodes.items()
                                   if self.node_shard_count[node_id] < self.max_shards_per_node]
                if len(available_nodes) < Config.REPLICATION_FACTOR:
                    # Not enough nodes with capacity, add new nodes
                    new_nodes = self.add_new_nodes()
                    available_nodes.extend(new_nodes)
                # Limit to replication factor
                available_nodes = available_nodes[:Config.REPLICATION_FACTOR]
                # Assign shard to nodes
                self.shard_to_nodes[shard_id] = [node.node_id for node in available_nodes]
                for node in available_nodes:
                    self.node_shard_count[node.node_id] += 1
            else:
                # Shard exists, get nodes responsible
                node_ids = self.shard_to_nodes[shard_id]
                available_nodes = [self.nodes[node_id] for node_id in node_ids]
            # Replicate data across nodes responsible for the shard
            self.replicate_data_across_nodes(name, email, data, shard_id, available_nodes)
            logging.info(f"Inserted data into shard {shard_id}")
        except Exception as e:
            logging.error(f"Error inserting data: {e}")

    def replicate_data_across_nodes(self, name, email, data, shard_id, nodes):
        for node in nodes:
            table_name = f"shard_{shard_id}"
            node.sqlite_manager.create_table(table_name)
            node.replicate_log_entries([data])
            node.apply_committed_entries(node.sqlite_manager, table_name)
            node.sqlite_manager.insert_record(name, email, data, table_name)

    def query_data(self):
        try:
            records = []
            for node in self.nodes.values():
                records.extend(node.sqlite_manager.query_records())
            logging.info("Queried data successfully")
            return records
        except Exception as e:
            logging.error(f"Error querying data: {e}")
            return []

    def add_node(self, node_id):
        try:
            if node_id not in self.nodes:
                db_path = f"node_{node_id}.db"
                raft_node = RaftNode(node_id, db_path)
                self.nodes[node_id] = raft_node
                self.node_shard_count[node_id] = 0
                logging.info(f"Added node {node_id} to the distributed system")
        except Exception as e:
            logging.error(f"Error adding node {node_id}: {e}")

    def add_new_nodes(self):
        new_nodes = []
        new_node_ids = range(len(self.nodes) + 1, len(self.nodes) + Config.REPLICATION_FACTOR + 1)
        for new_node_id in new_node_ids:
            self.add_node(new_node_id)
            new_nodes.append(self.nodes[new_node_id])
        logging.info(f"Scaled horizontally by adding nodes {list(new_node_ids)}")
        self.start_raft_nodes(new_nodes)
        self.conduct_new_election(new_nodes)
        return new_nodes

    def start_raft_nodes(self, nodes=None):
        nodes = nodes or self.nodes.values()
        for node in nodes:
            node.start()

    def conduct_new_election(self, nodes=None):
        nodes = nodes or self.nodes.values()
        for node in nodes:
            node.handle_leader_election()

def main():
    try:
        db = DistributedDatabase("distributed.db", 1)

        # Add initial nodes to meet the replication factor
        for i in range(2, Config.REPLICATION_FACTOR + 1):
            db.add_node(i)

        # Start RAFT nodes
        db.start_raft_nodes()

        # Insert data and create new shards
        for i in range(80):
            db.insert_data(f"Name{i}", f"email{i}@example.com", f"Sample data {i}")
        
        # Query data
        print(db.query_data())

        # Show distribution of shards across nodes
        logging.info("Shard distribution across nodes:")
        for node_id, node in db.nodes.items():
            logging.info(f"Node {node_id}: {node.log}")

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    main()