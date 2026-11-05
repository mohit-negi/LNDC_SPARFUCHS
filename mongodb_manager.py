
from pymongo import MongoClient
from datetime import datetime


class MongoDBManager:
    def __init__(self, connection_string, database_name, collection_name):
        self.client = MongoClient(connection_string)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

        # Verify database connection
        self.client.admin.command("ping")
        print("Successfully connected to MongoDB")

    def insert_data(self, data):
        document = {
            "message": data,
            "timestamp": datetime.now()
        }

        result = self.collection.insert_one(document)

        print(f"Data inserted with ID: {result.inserted_id}")

    def get_all_data(self):
        return list(self.collection.find({}, {"_id": 0}))

    def close(self):
        self.client.close()
        print("MongoDB connection closed")
