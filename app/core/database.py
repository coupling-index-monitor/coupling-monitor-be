from neo4j import GraphDatabase
from app.core.config import settings


class DatabaseManager:
    def __init__(self):
        # Neo4j driver
        self.neo4j_driver = None

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

# Instantiate a global DatabaseManager
db_manager = DatabaseManager()
