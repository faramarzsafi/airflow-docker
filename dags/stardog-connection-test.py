import stardog

STARDOG_SERVER = "https://surround-aus.stardog.cloud:5820"
DB_NAME = "utswkflowsand"
USERNAME = "faramarz.safiesfahani@student.uts.edu.au"
PASSWORD = "Fara1234@stard0g"

try:
    connection_details = {
        'endpoint': STARDOG_SERVER,
        'username': USERNAME,
        'password': PASSWORD
    }
    with stardog.Connection(DB_NAME, **connection_details) as conn:
        print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
