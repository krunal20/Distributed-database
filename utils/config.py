# Configuration utilities for the distributed database application

class Config:
    INITIAL_NODES = 1
    REPLICATION_FACTOR = 3
    SHARD_SIZE = 10
    MAX_SHARDS_PER_NODE = 3

    def __init__(self, config_path):
        self.config_path = config_path

    def load(self):
        print(f"Loading configuration from {self.config_path}")

def generate_unique_id():
    generate_unique_id.counter += 1
    return generate_unique_id.counter

generate_unique_id.counter = 0