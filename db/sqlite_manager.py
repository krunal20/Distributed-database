import sqlite3
import logging

# SQLite manager for handling SQLite database operations

class SQLiteManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def connect(self):
        logging.info(f"Connecting to SQLite database at {self.db_path}")
        return sqlite3.connect(self.db_path)

    def create_database(self):
        try:
            conn = self.connect()
            conn.close()
            logging.info(f"Created SQLite database at {self.db_path}")
        except Exception as e:
            logging.error(f"Error creating database: {e}")

    def create_table(self, table_name):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    email TEXT,
                    data TEXT
                )
            ''')
            conn.commit()
            conn.close()
            logging.info(f"Created table '{table_name}'")
        except Exception as e:
            logging.error(f"Error creating table '{table_name}': {e}")

    def insert_record(self, name, email, data, table_name):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f'''
                INSERT INTO {table_name} (name, email, data) VALUES (?, ?, ?)
            ''', (name, email, data))
            conn.commit()
            conn.close()
            logging.info(f"Inserted record into '{table_name}' table: {name}, {email}, {data}")
        except Exception as e:
            logging.error(f"Error inserting record into '{table_name}': {e}")

    def query_records(self):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
            tables = cursor.fetchall()
            records = []
            for table in tables:
                table_name = table[0]
                cursor.execute(f'SELECT * FROM {table_name}')
                records.extend(cursor.fetchall())
            conn.close()
            logging.info("Queried records from all tables")
            return records
        except Exception as e:
            logging.error(f"Error querying records: {e}")
            return []