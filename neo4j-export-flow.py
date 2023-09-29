from prefect import flow, task, get_run_logger
from neo4j_exporter import *

@flow(name="Neo4J Export Data Flow",log_prints=True)
def neo4j_offload(nodes_file_path, relations_file_path):
    # Setting it to an environment variable right now
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    if (uri == None) or (user == None) or (password == None):
        raise("Either URI, Username or Password is not set")
    try:
        # Initialize the Class
        neo4j_export_processor = Neo4JExportProcessor(uri, user, password)
        #Execute the export
        neo4j_export_processor.export_nodes_relations(nodes_file_path,relations_file_path)
        print("Data was successfully exported")
    except Exception as e:
        print("Exception Occurred when exporting: ",e)



if __name__ == "__main__":
    
    # Export the Data
    date_time_str = datetime.datetime.now().strftime("%Y-%m-%dT-%H-%M-%S")
    node_file = f"nodes-{date_time_str}.tsv"
    rel_file = f"rel-{date_time_str}.tsv"
    neo4j_offload(node_file,rel_file)