from prefect import flow, task, get_run_logger
from neo4j_importer import *

@flow(name="Neo4J Load Data Flow",log_prints=True)
def neo4j_load(nodes_file_path, relations_file_path):
    # Setting it to an environment variable right now
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if (uri == None) or (user == None) or (password == None):
        raise("Either URI, Username or Password is not set")
    try:
        # Initialize the Class
        neo4j_import_processor = Neo4JImportProcessor(uri, user, password)
        #Execute the flow
        neo4j_import_processor.import_nodes_relations(nodes_file_path,relations_file_path)
        print("Data was successfully loaded")
    except Exception as e:
        print("Exception Occurred when loading: ",e)



if __name__ == "__main__":
    
    # These are sample files from the Movies Dataset 
    nodes_file_path = "nodes-2023-09-28T-10-23-50.tsv"
    relations_file_path = "rel-2023-09-28T-10-23-50.tsv"
    neo4j_load(nodes_file_path,relations_file_path)