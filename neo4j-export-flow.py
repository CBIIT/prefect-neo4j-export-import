from prefect import flow, task, get_run_logger,variables
from neo4j_exporter import *
import boto3

'''
# Gets the DB Connection Data from Parameter Store. If run locally, expects to read from
Localstack endpoint from an environment variable. Flow Needs to be provided the parameter values
from AWS SSM Parameter Store
'''
@task(name="Get DB Connection Params")
def get_conn_params(uri_param_name, user_param_name,password_param_name):
    
    uri = user = password = None
    # Hardcoding temporarily
    REGION = "us-east-1"
    print("Region is: ",REGION)
    localstack_url = os.getenv("LOCALSTACK_ENDPOINT_URL")
    if(localstack_url != None):
        
        ssm = boto3.client('ssm',endpoint_url=localstack_url,region_name=REGION)
    else :
        ssm = boto3.client('ssm',region_name=REGION)

    try:
        uri = ssm.get_parameter(Name=uri_param_name)['Parameter']['Value']
        user = ssm.get_parameter(Name=user_param_name)['Parameter']['Value']
        password = ssm.get_parameter(Name=password_param_name)['Parameter']['Value']
        
    except Exception as e:
        print("Exception Occurred when exporting: ",e)
    return (uri,user,password)

'''
# Runs the main flow. Retrieves the Connection information calls Neo4J Export Processor
'''

@flow(name="Neo4J Export Data Flow",log_prints=True)
def neo4j_offload(nodes_file_path, relations_file_path,uri_param_name="NEO4J_URI", user_param_name="NEO4J_USER", password_param_name="NEO4J_PASSWORD"):
    # Setting it to an environment variable right now
    uri,user,password = get_conn_params(uri_param_name,user_param_name,password_param_name)
    if (uri == None) or (user == None) or (password == None):
        raise Exception("Either URI, Username or Password is not set")
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