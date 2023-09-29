from neo4j import GraphDatabase
import csv, json, ast, os
"""
This is the main class
"""
class Neo4JImportProcessor:
    def __init__(self, uri, user, password):
        # Connect to Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    #######
    # This is the method that loads the nodes and relationships
    #######
    def import_nodes_relations(self,nodes_file_path,relations_file_path):
        # Load nodes
        # Define the file paths
        node_file=nodes_file_path
        rel_file=relations_file_path
        driver=self.driver
        

        with open(node_file, 'r', newline='') as node_csv:
            node_reader = csv.reader(node_csv, delimiter='\t')
            with driver.session() as session:
                for row in node_reader:
                    # Assumes that NodeID and Label are first and second elements in CSV file
                    node_id, node_label = row[0], row[1]
                    # Converts Non Quoted JSON to Python Dictionary Object
                    node_props = ast.literal_eval(row[2])
                    # Creates the Nodes
                    session.run(f"CREATE (:{node_label} $props)", props=(node_props))

        # Load relationships
        with open(rel_file, 'r', newline='') as rel_csv:
            rel_reader = csv.reader(rel_csv, delimiter='\t')
            with driver.session() as session:
                for row in rel_reader:
                    # Assumes that Node_a_ID , Node_rel_type and Node_b_ID are the first, second and third items in CSV file
                    node_a_id,rel_type,node_b_id = row[0],row[1],row[2]
                    session.run(f"MATCH (a),(b) WHERE ID(a)={node_a_id} AND ID(b)={node_b_id} CREATE (a)-[:{rel_type}]->(b)")


                  

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if (uri == None) or (user == None) or (password == None):
        raise("Either URI, Username or Password is not set")
    # Initialize the Class
    neo4j_import_processor = Neo4JImportProcessor(uri, user, password)
    # Import the Data into DB
    node_file = "nodes-2023-09-28T-10-23-50.tsv"
    rel_file = "rel-2023-09-28T-10-23-50.tsv"
    # Import Nodes and Relationships into DB
    neo4j_import_processor.import_nodes_relations(node_file,rel_file)