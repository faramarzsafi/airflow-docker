from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
import json
import requests
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import XSD
from jsonschema import validate, ValidationError
from requests.auth import HTTPBasicAuth

# Default arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Stardog API Configuration (As Provided)
STARDOG_SERVER = "https://surround-aus.stardog.cloud:5820"
DB_NAME = "utswkflowsand"
USERNAME = "faramarz.safiesfahani@student.uts.edu.au"
PASSWORD = "Fara@stard0g"

# Namespaces for RDF
PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/")

# Provenance Schema Validation
PROVENANCE_SCHEMA = {
    "type": "object",
    "properties": {
        "workflow_name": {"type": "string"},
        "description": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "entities": {
            "type": "array",
            "items": {"type": "object", "properties": {"id": {"type": "string"}, "label": {"type": "string"}}}
        },
        "activities": {
            "type": "array",
            "items": {"type": "object", "properties": {"id": {"type": "string"}, "label": {"type": "string"}}}
        },
        "agents": {
            "type": "array",
            "items": {"type": "object", "properties": {"id": {"type": "string"}, "label": {"type": "string"}}}
        }
    },
    "required": ["workflow_name", "description", "timestamp", "entities", "activities", "agents"]
}

# Function: Generate Provenance Data and Store in XCom
def generate_provenance(**kwargs):
    try:
        provenance_data = {
            "workflow_name": "Hello World Workflow",
            "description": "A workflow generating and tracking provenance using PROV-O.",
            "timestamp": pendulum.now().to_iso8601_string(),
            "entities": [
                {"id": "ex:dataset1", "label": "Dataset fetched from Stardog"},
                {"id": "ex:output1", "label": "Processed output result"}
            ],
            "activities": [
                {"id": "ex:fetch_data", "label": "Fetch data from Stardog"},
                {"id": "ex:process_data", "label": "Processing Data"}
            ],
            "agents": [
                {"id": "ex:airflow_dag", "label": "Airflow DAG"},
                {"id": "ex:user_faramarz", "label": "Faramarz"}
            ]
        }

        # Validate JSON
        validate(instance=provenance_data, schema=PROVENANCE_SCHEMA)

        # Push data to XCom
        kwargs['ti'].xcom_push(key='provenance_data', value=provenance_data)

        print("✅ Provenance data generated and stored in XCom.")
    except ValidationError as e:
        print(f"🚨 Provenance validation error: {e}")
    except Exception as e:
        print(f"⚠️ Error generating provenance: {e}")

# Function: Convert JSON to RDF (PROV-O Ontology)
def convert_json_to_rdf(json_data):
    try:
        g = Graph()
        g.bind("prov", PROV)
        g.bind("ex", EX)

        # Workflow Activity
        workflow_uri = URIRef(EX[json_data['workflow_name'].replace(" ", "_")])
        g.add((workflow_uri, RDF.type, PROV.Activity))
        g.add((workflow_uri, PROV.label, Literal(json_data['workflow_name'], datatype=XSD.string)))

        # Entities
        for entity in json_data['entities']:
            entity_uri = URIRef(entity["id"])
            g.add((entity_uri, RDF.type, PROV.Entity))
            g.add((entity_uri, PROV.label, Literal(entity["label"], datatype=XSD.string)))

        # Activities
        for activity in json_data['activities']:
            activity_uri = URIRef(activity["id"])
            g.add((activity_uri, RDF.type, PROV.Activity))
            g.add((activity_uri, PROV.label, Literal(activity["label"], datatype=XSD.string)))

        # Agents
        for agent in json_data['agents']:
            agent_uri = URIRef(agent["id"])
            g.add((agent_uri, RDF.type, PROV.Agent))
            g.add((agent_uri, PROV.label, Literal(agent["label"], datatype=XSD.string)))

        return g.serialize(format="nt")
    except Exception as e:
        print(f"⚠️ Error converting JSON to RDF: {e}")
        return ""

# Function: Upload RDF to Stardog via SPARQL API
def upload_rdf_to_stardog(**kwargs):
    try:
        ti = kwargs['ti']
        provenance_data = ti.xcom_pull(task_ids='generate_provenance', key='provenance_data')

        if not provenance_data:
            print("🚨 No provenance data found in XCom.")
            return

        rdf_data = convert_json_to_rdf(provenance_data)

        # Stardog SPARQL Update Endpoint
        sparql_endpoint = f"{STARDOG_SERVER}/{DB_NAME}/update"

        # SPARQL Insert Query
        insert_query = f"""
        INSERT DATA {{
            {rdf_data}
        }}
        """

        headers = {
            "Content-Type": "application/sparql-update"
        }

        response = requests.post(
            sparql_endpoint,
            data=insert_query,
            headers=headers,
            auth=HTTPBasicAuth(USERNAME, PASSWORD)
        )

        if response.status_code == 200:
            print("✅ Provenance successfully uploaded to Stardog.")
        else:
            print(f"🚨 Error uploading to Stardog: {response.text}")

    except Exception as e:
        print(f"🚨 Error uploading to Stardog: {e}")

# Define Airflow DAG
with DAG(
    'prov_o_airflow_workflow',
    default_args=default_args,
    description='Airflow workflow using PROV-O ontology for provenance tracking.',
    schedule=None,
    catchup=False,
) as dag:

    generate_provenance_task = PythonOperator(
        task_id='generate_provenance',
        python_callable=generate_provenance,
        provide_context=True,
    )

    upload_rdf_to_stardog_task = PythonOperator(
        task_id='upload_rdf_to_stardog',
        python_callable=upload_rdf_to_stardog,
        provide_context=True,
    )

    trigger_rust_ui_task = SimpleHttpOperator(
        task_id="trigger_rust_ui",
        http_conn_id="rust_ui_conn",
        endpoint="/update",
        method="POST",
        data=json.dumps({"status": "Workflow completed"}),
        headers={"Content-Type": "application/json"},
    )

    generate_provenance_task >> upload_rdf_to_stardog_task >> trigger_rust_ui_task
