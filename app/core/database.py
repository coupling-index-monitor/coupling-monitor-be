from neo4j import GraphDatabase
from app.core.config import settings
import certifi
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, CollectionInvalid


class DatabaseManager:
    def __init__(self):
        # Neo4j driver
        self.neo4j_driver = None

        self.mongo_client = None
        self.metrics_collection = None
        self.metric_updates_collection = None

    def initialize_neo4j(self):
        """
        Initialize Neo4j connection.
        """
        try:
            # Connect to Neo4j
            self.neo4j_driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )
            self.neo4j_driver.verify_connectivity()
            print("Neo4j connected successfully.")
        except Exception as e:
            print(f"Neo4j connection failed: {e}")
            raise e

    def close_neo4j(self):
        """
        Close Neo4j connection.
        """
        if self.neo4j_driver is not None:
            self.neo4j_driver.close()
            print("Neo4j connection closed.")
        else:
            print("Neo4j driver was not initialized.")

    async def initialize_mongo(self):
        """
        Initialize MongoDB connection and collections.
        """
        try:
            # Connect to MongoDB
            self.mongo_client = MongoClient(settings.MONGO_URI, tlsCAFile=certifi.where())
            self.mongo_client.admin.command("ping")
            db = self.mongo_client[settings.MONGO_DB]

            if settings.MONGO_METRICS_COLLECTION not in db.list_collection_names():
                db.create_collection(settings.MONGO_METRICS_COLLECTION)
            if settings.MONGO_METRIC_UPDATELOG_COLLECTION not in db.list_collection_names():
                db.create_collection(settings.MONGO_METRIC_UPDATELOG_COLLECTION)

            self.metrics_collection = db[settings.MONGO_METRICS_COLLECTION]
            self.metric_updates_collection = db[settings.MONGO_METRIC_UPDATELOG_COLLECTION]

            print(f"MongoDB connected successfully. Collections initialized:")
        except ConnectionFailure as e:
            print(f"MongoDB connection failed: {e}")
            raise e

    async def close_mongo(self):
        """
        Close MongoDB connection.
        """
        if self.mongo_client is not None:
            self.mongo_client.close()
            print("MongoDB connection closed.")
        else:
            print("MongoDB client was not initialized.")

    def get_metrics_collection(self):
        """
        Get MongoDB 'metrics' collection.
        """
        if self.metrics_collection is None:
            raise RuntimeError("MongoDB 'metrics' collection is not initialized.")
        return self.metrics_collection
    
    def get_metric_updates_collection(self):
        """
        Get MongoDB 'metric_updates' collection.
        """
        if self.metric_updates_collection is None:
            raise RuntimeError("MongoDB 'metric_updates' collection is not initialized.")
        return self.metric_updates_collection
    
# Instantiate a global DatabaseManager
db_manager = DatabaseManager()
