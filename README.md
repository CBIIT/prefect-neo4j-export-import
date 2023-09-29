# prefect-neo4j-export-import
A Sample repository which exports and imports nodes and relationships from neo4j and runs as prefect flows

# neo4j_importer.py
Used to load nodes and relationships into Neo4J. Meant for Local Testing.And to be used as a module.
# neo4j_exporter.py
Used to export nodes and relationships from Neo4J into timestamped TSV files. Meant for Local Testing.And to be used as a module.

# neo4j-import-flow.py
Used to run a prefect flow to load Neo4J using neo4j_importer module.

# neo4j-export-flow.py
Used to run a prefect flow to load Neo4J using neo4j_exporter module.

# Environment Variables
NEO4J_URI = bolt://localhost:7687 ({protocol}://{ipv4}:{port})
NEO4J_USER=neo4j (db username)
NEO4J_PASSWORD=password (db password)

