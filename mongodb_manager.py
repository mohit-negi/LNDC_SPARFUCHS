
import logging
import re

from pymongo import MongoClient, ASCENDING, DESCENDING
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
        
    def list_sensors(self):
        return sorted(
            self.database.list_collection_names()
        )


    def sensor_exists(self, sensor_id):
        return sensor_id in self.database.list_collection_names()


    def get_latest_reading(self, sensor_id):

        if not self.sensor_exists(sensor_id):
            return None

        collection = self.database[sensor_id]

        document = collection.find_one(
            {},
            sort=[("timestamp", DESCENDING)]
        )

        if document:
            document["id"] = str(document.pop("_id"))

        return document


    def get_sensor_readings(
        self,
        sensor_id,
        start=None,
        end=None,
        limit=100,
        skip=0
    ):

        if not self.sensor_exists(sensor_id):
            return None

        collection = self.database[sensor_id]

        query = {}

        if start or end:
            query["timestamp"] = {}

            if start:
                query["timestamp"]["$gte"] = start

            if end:
                query["timestamp"]["$lte"] = end

        cursor = (
            collection.find(query)
            .sort("timestamp", DESCENDING)
            .skip(skip)
            .limit(limit)
        )

        results = []

        for document in cursor:
            document["id"] = str(document.pop("_id"))
            results.append(document)

        return results

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
