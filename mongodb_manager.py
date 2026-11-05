
import logging
import re

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError


logger = logging.getLogger(__name__)


class MongoDBManager:

    def __init__(self, connection_string, database_name):

        self.client = MongoClient(
            connection_string,
            serverSelectionTimeoutMS=5000
        )

        self.database = self.client[database_name]

        # Verify connection immediately
        self.client.admin.command("ping")

        self.collections = {}

        logger.info("MongoDB connected successfully")

    def get_sensor_collection(self, sensor_id):

        # Prevent unsafe collection names
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", sensor_id):
            raise ValueError("Invalid sensor ID")

        if sensor_id not in self.collections:

            collection = self.database[sensor_id]

            # Index timestamp for efficient time-based queries
            collection.create_index(
                [("timestamp", ASCENDING)]
            )

            self.collections[sensor_id] = collection

        return self.collections[sensor_id]

    def insert_sensor_data(self, sensor_id, document):

        try:
            collection = self.get_sensor_collection(sensor_id)

            result = collection.insert_one(document)

            logger.info(
                "Inserted data for %s: %s",
                sensor_id,
                result.inserted_id
            )

            return result.inserted_id

        except PyMongoError:
            logger.exception(
                "MongoDB insertion failed for %s",
                sensor_id
            )
            raise

    def close(self):

        self.client.close()

        logger.info("MongoDB connection closed")
