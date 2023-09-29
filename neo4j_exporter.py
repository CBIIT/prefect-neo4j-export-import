from neo4j import GraphDatabase
import csv,datetime,os
"""
This is the main class
"""
class Neo4JExportProcessor:
    def __init__(self, uri, user, password):
        # Connect to Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    #######
    # This is the method that exports the nodes and relationships to a TSV
    #######    
    def export_nodes_relations(self,nodes_file_path,relations_file_path):
        # Get Node and Relationships file pths
        node_file, rel_file = nodes_file_path,relations_file_path
        # Get Current Datetime in Y-M-D-H-M-S format
        date_time_str = datetime.datetime.now().strftime("%Y-%m-%dT-%H-%M-%S")
        # If node or relationships paths are not defined create them
        if(nodes_file_path is None):
            node_file = f"nodes-{date_time_str}.tsv"

        if(relations_file_path is None):    
            rel_file = f"rel-{date_time_str}.tsv"

        driver=self.driver        
        # Export nodes
        with open(node_file, 'w', newline='') as node_csv:
            node_writer = csv.writer(node_csv, delimiter='\t')
            with driver.session() as session:
                nodes = session.run("MATCH (n) RETURN n")
                for node in nodes:
                    node_writer.writerow([node[0].id, list(node[0].labels)[0], dict(node[0])])

        # Export relationships
        with open(rel_file, 'w', newline='') as rel_csv:
            rel_writer = csv.writer(rel_csv, delimiter='\t')
            with driver.session() as session:
                rels = session.run("MATCH (a)-[r]->(b) RETURN id(a), type(r), id(b)")
                for rel in rels:
                    rel_writer.writerow([rel[0], rel[1], rel[2]])

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if (uri == None) or (user == None) or (password == None):
        raise("Either URI, Username or Password is not set")
    # Initialize the Class
    neo4j_export_processor = Neo4JExportProcessor(uri, user, password)
    # Export the Data
    date_time_str = datetime.datetime.now().strftime("%Y-%m-%dT-%H-%M-%S")
    node_file = f"nodes-{date_time_str}.tsv"
    rel_file = f"rel-{date_time_str}.tsv"  
    # Export Nodes and Relationships
    neo4j_export_processor.export_nodes_relations(node_file,rel_file)                  