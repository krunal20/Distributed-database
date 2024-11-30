import logging

# Shard manager for handling database sharding operations

class ShardManager:
    def __init__(self, shards=None):
        self.shards = shards if shards is not None else {}
        self.shard_size = 10

    def create_shard(self, shard_id):
        try:
            start_range = shard_id * self.shard_size
            end_range = start_range + self.shard_size - 1
            self.shards[shard_id] = (start_range, end_range)
            logging.info(f"Created shard {shard_id} with range ({start_range}, {end_range})")
        except Exception as e:
            logging.error(f"Error creating shard {shard_id}: {e}")

    def determine_shard(self, record_id):
        try:
            # for shard_id, (start_range, end_range) in self.shards.items():
            #     if start_range <= record_id <= end_range:
            #         return shard_id
            if self.shards.get(record_id//10) is not None:
                return record_id//10
            else:
                return None
        except Exception as e:
            logging.error(f"Error determining shard for record ID {record_id}: {e}")
            return None

    def distribute_data(self, data):
        try:
            logging.info(f"Distributing data across {len(self.shards)} shards")
            for record_id, record_data in enumerate(data):
                shard_id = self.determine_shard(record_id)
                if shard_id is None:
                    shard_id = len(self.shards)
                    self.create_shard(shard_id)
                logging.info(f"Record {record_id} assigned to shard {shard_id}")
        except Exception as e:
            logging.error(f"Error distributing data: {e}")