from datetime import datetime
import networkx as nx
from networkx.readwrite import json_graph
from app.core.database import db_manager

def update_graph_in_neo4j(graph):
    """
    Update the dependency graph in Neo4j using the provided NetworkX graph.
    """
    with db_manager.neo4j_driver.session() as session:
        for edge in graph.edges(data=True):
            parent, child = edge

            # Create or update nodes and relationships in Neo4j
            session.run("""
                MERGE (a:Service {name: $parent})
                MERGE (b:Service {name: $child})
                MERGE (a)-[r:CALLS]->(b)
            """, parent=parent, child=child)


def fetch_graph_from_neo4j():
    """
    Fetch the dependency graph from Neo4j and return it as a NetworkX graph object.
    """
    graph = nx.DiGraph()

    try:
        with db_manager.neo4j_driver.session() as session:
            result = session.run("""
                MATCH (a:Service)-[r:CALLS]->(b:Service)
                RETURN a.name AS parent, b.name AS child
            """)
            for record in result:
                graph.add_edge(record["parent"], record["child"])

    except Exception as e:
        print(f"Error fetching graph from Neo4j: {e}")

    return graph

def fetch_unique_services_from_neo4j():
    """
    Fetch the unique services from Neo4j and return them as a list.
    """
    services = set()
    try:
        with db_manager.neo4j_driver.session() as session:
            result = session.run("""
                MATCH (s:Service)
                RETURN DISTINCT s.name AS service
            """)
            for record in result:
                services.add(record["service"])
    except Exception as e:
        print(f"Error fetching unique services from Neo4j: {e}")
    return list(services)


def get_graph_data_as_json():
    """
    Retrieve the dependency graph as JSON-compatible data for API or frontend consumption.
    """
    graph = fetch_graph_from_neo4j()
    return json_graph.node_link_data(graph)


def save_graph_to_neo4j(graph_data, startTime, endTime):
    """Saves multiple graphs in Neo4j using graph_id (timestamp/version)."""
    if endTime is None or startTime is None:
        raise ValueError("Start and End time must be provided.")

    with db_manager.neo4j_driver.session() as session:
        for node in graph_data["data"]["nodes"]:
            session.run(
                """
                MERGE (s:Service {id: $id, graph_id: $graph_id})
                SET s.absolute_importance = $absolute_importance,
                    s.absolute_dependence = $absolute_dependence,
                    s.last_updated = datetime()
                """,
                id=node["id"],
                graph_id=endTime,
                absolute_importance=node["absolute_importance"],
                absolute_dependence=node["absolute_dependence"],
                startTime=startTime,
                endTime=endTime
            )

        # Insert Edges with graph_id
        for edge in graph_data["data"]["edges"]:
            session.run(
                """
                MATCH (source:Service {id: $source, graph_id: $graph_id}), 
                      (target:Service {id: $target, graph_id: $graph_id})
                MERGE (source)-[r:CALLS {graph_id: $graph_id}]->(target)
                SET r.latency = $latency,
                    r.frequency = $frequency,
                    r.co_execution = $co_execution
                """,
                source=edge["source"],
                target=edge["target"],
                graph_id=endTime,
                latency=edge["latency(ms)"],
                frequency=edge["frequency"],
                co_execution=edge["co_execution"],
                startTime=startTime,
                endTime=endTime
            )

    print(f"Graph {endTime} saved to Neo4j successfully.")
    return endTime

def retrieve_graph_by_id(id):
    """Retrieves a specific graph snapshot using graph_id."""
    
    print(f"Retrieving graph with graph_id: {id}")
    with db_manager.neo4j_driver.session() as session:
        # Retrieve Nodes
        nodes_result = session.run(
            "MATCH (s:Service) WHERE s.graph_id = $graph_id RETURN s.id AS id, s.absolute_importance AS ai, s.absolute_dependence AS ad",
            graph_id=int(id)
        )
        nodes = [{
            "id": record["id"], 
            "absolute_importance": record["ai"], 
            "absolute_dependence": record["ad"]
        } for record in nodes_result]

        # Retrieve Edges
        edges_result = session.run(
            "MATCH (s1:Service)-[r:CALLS {graph_id: $graph_id}]->(s2:Service) RETURN s1.id AS from, s2.id AS to, r.latency AS latency, r.frequency AS frequency, r.co_execution AS co_execution",
            graph_id=int(id)
        )
        edges = [{
            "source": record["from"], 
            "target": record["to"],
            "latency": record["latency"], 
            "frequency": record["frequency"], 
            "co_execution": record["co_execution"]
        } for record in edges_result]

    return {"nodes": nodes, "edges": edges}

def get_all_graph_versions():
    """Retrieves all stored graph versions (graph_ids)."""
    with db_manager.neo4j_driver.session() as session:
        result = session.run("MATCH (s:Service) RETURN DISTINCT s.graph_id AS graph_id")
        graph_versions = [record["graph_id"] for record in result]

    return graph_versions