from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum
import json
import os

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Obstacle points as a list of dictionaries
OBSTACLES = [
    {"obsx": 143.747646, "obsy": -40.992829},
    {"obsx": 144.001, "obsy": -41.123},
    {"obsx": 145.678, "obsy": -42.345}  # Replace with actual obstacle points as needed
]

# File path for provenance data
PROVENANCE_PATH = './provenance.json'  # Changed to a local directory for easier testing on Windows

# Ensure the directory exists
os.makedirs(os.path.dirname(PROVENANCE_PATH), exist_ok=True)

# Function to generate provenance data
def generate_provenance():
    provenance_data = {
        "workflow_name": "Hello World",
        "description": "Workflow to process and demonstrate obstacle points.",
        "obstacles": OBSTACLES,
    }
    with open(PROVENANCE_PATH, 'w') as file:
        json.dump(provenance_data, file)
    print(f"Provenance data generated and saved as JSON at {PROVENANCE_PATH}.")

# Function to load data into Stardog KG
def load_to_kg():
    with open(PROVENANCE_PATH, 'r') as file:
        data = json.load(file)
    print("Loading the following data into Stardog KG:", data)

# Function to trigger Rust UI
def trigger_rust_ui():
    print("Triggering Rust UI to display data.")

# Define the DAG
with DAG(
    'custom_hello_world_workflow',
    default_args=default_args,
    description='A customized Airflow workflow with JSON communication and provenance input.',
    schedule=None,
    catchup=False,
) as dag:

    # Task to generate provenance data
    generate_provenance_task = PythonOperator(
        task_id='generate_provenance',
        python_callable=generate_provenance,
    )

    # Task to commit changes to Git
    commit_to_git_task = BashOperator(
        task_id='commit_to_git',
        bash_command=(
            'cd /opt/airflow/repo && '
            'git add . && '
            'git commit -m "Automated provenance commit with obstacle points." && '
            'git push origin main'
        ),
    )

    # Task to load data into Stardog KG
    load_to_kg_task = PythonOperator(
        task_id='load_to_kg',
        python_callable=load_to_kg,
    )

    # Task to trigger Rust UI
    trigger_rust_ui_task = PythonOperator(
        task_id='trigger_rust_ui',
        python_callable=trigger_rust_ui,
    )

    # Define task dependencies
    generate_provenance_task >> commit_to_git_task >> load_to_kg_task >> trigger_rust_ui_task

# Add the __main__ block for testing individual functions
if __name__ == "__main__":
    print("Running individual functions for testing:")
    generate_provenance()
    load_to_kg()
    trigger_rust_ui()
