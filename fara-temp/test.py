from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import json
import os
import stardog
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import RDFS
from urllib.parse import quote

# Default arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Define constants
PROVENANCE_PATH = "C:/Users/25640715/GitRepo/RAN/data/provenance.json"
STARDOG_SERVER = "https://surround-aus.stardog.cloud:5820"
DB_NAME = "utswkflowsand"
USERNAME = "faramarz.safiesfahani@student.uts.edu.au"
PASSWORD = "Fara@stard0g"

# Ensure necessary directories exist
os.makedirs(os.path.dirname(PROVENANCE_PATH), exist_ok=True)


# Function: Generate Provenance Data
def generate_provenance():
    try:
        print(f"Debug: Generating provenance data at {PROVENANCE_PATH}...")
        provenance_data = {
            "workflow_name": "Hello World",
            "description": "Workflow to process and demonstrate obstacle points.",
            "timestamp": pendulum.now().to_iso8601_string(),
            "obstacles": [
                {"obsx": 143.747646, "obsy": -40.992829},
                {"obsx": 144.001, "obsy": -41.123},
                {"obsx": 145.678, "obsy": -42.345},
            ],
        }

        with open(PROVENANCE_PATH, 'w') as file:
            json.dump(provenance_data, file)

        print(f"Debug: Provenance data written to {PROVENANCE_PATH}.")
    except Exception as e:
        print(f"Error generating provenance data: {e}")


# Function: Encode URIs safely
def safe_uri(uri):
    return URIRef(quote(uri, safe=":/#"))


# Function: Convert JSON to RDF/XML
def convert_json_to_rdf(json_data):
    try:
        g = Graph()
        EX = Namespace("http://example.org/")
        g.bind("ex", EX)

        # Add workflow details
        workflow_name = quote(json_data['workflow_name'].replace(' ', '_'))
        workflow = safe_uri(f"http://example.org/workflow/{workflow_name}")
        g.add((workflow, RDF.type, EX.Workflow))
        g.add((workflow, EX.name, Literal(json_data['workflow_name'])))
        g.add((workflow, EX.description, Literal(json_data['description'])))
        g.add((workflow, EX.timestamp, Literal(json_data['timestamp'])))
        g.add((workflow, RDFS.label, Literal(json_data['workflow_name'])))

        # Add obstacles
        for obstacle in json_data['obstacles']:
            obstacle_uri = safe_uri(f"http://example.org/obstacle/{obstacle['obsx']}_{obstacle['obsy']}")
            g.add((obstacle_uri, RDF.type, EX.Obstacle))
            g.add((obstacle_uri, EX.obsx, Literal(obstacle['obsx'])))
            g.add((obstacle_uri, EX.obsy, Literal(obstacle['obsy'])))
            g.add((workflow, EX.hasObstacle, obstacle_uri))
            g.add((obstacle_uri, RDFS.label, Literal(f"Obstacle at {obstacle['obsx']}, {obstacle['obsy']}")))

        # Serialize to N-Triples for SPARQL compatibility
        return g.serialize(format="nt")
    except Exception as e:
        print(f"Error converting JSON to RDF: {e}")
        return ""


# Function: Upload RDF data to Stardog
def upload_rdf_to_stardog(server, db_name, username, password, rdf_data):
    try:
        connection_details = {
            'endpoint': server,
            'username': username,
            'password': password,
        }

        with stardog.Connection(db_name, **connection_details) as conn:
            conn.begin()
            insert_query = f"""
            INSERT DATA {{
                {rdf_data}
            }}
            """
            conn.update(insert_query)
            conn.commit()
        print("Provenance data successfully uploaded to Stardog.")
    except Exception as e:
        print(f"Error uploading data to Stardog: {e}")


# Function: Load Data into Stardog KG
def load_to_kg():
    try:
        print(f"Step 8: Loading data from {PROVENANCE_PATH} into Stardog KG...")
        with open(PROVENANCE_PATH, 'r') as file:
            provenance_data = json.load(file)

        # Convert JSON to RDF
        rdf_data = convert_json_to_rdf(provenance_data)

        # Upload RDF data to Stardog
        upload_rdf_to_stardog(STARDOG_SERVER, DB_NAME, USERNAME, PASSWORD, rdf_data)
    except Exception as e:
        print(f"Error in load_to_kg: {e}")


# Function: Trigger Rust UI
def trigger_rust_ui():
    print("Rust UI triggered to display workflow-generated data.")


# Define the Airflow DAG
with DAG(
        'hello_world_workflow',
        default_args=default_args,
        description='A workflow replacing static provenance files with workflow-generated provenance.',
        schedule=None,
        catchup=False,
) as dag:
    # Generate provenance task
    generate_provenance_task = PythonOperator(
        task_id='generate_provenance',
        python_callable=generate_provenance,
    )

    # Load data into Stardog KG task
    load_to_kg_task = PythonOperator(
        task_id='load_to_kg',
        python_callable=load_to_kg,
    )

    # Trigger Rust UI task
    trigger_rust_ui_task = PythonOperator(
        task_id='trigger_rust_ui',
        python_callable=trigger_rust_ui,
    )

    # Define task dependencies
    generate_provenance_task >> load_to_kg_task >> trigger_rust_ui_task


# Main block for testing
if __name__ == "__main__":
    print("Testing workflow tasks...")
    generate_provenance()
    load_to_kg()
    trigger_rust_ui()
