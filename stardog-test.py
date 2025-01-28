import requests

def debug_stardog_connection(endpoint, authorization_header):
    """
    Debug Stardog connection by inspecting request and response details.
    """
    # API URL to fetch databases
    url = f"{endpoint}/admin/databases"

    # Headers for authentication
    headers = {
        'Authorization': authorization_header,
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,en-AU;q=0.8',
        'Connection': 'close',
        'Sec-Ch-Ua': '"Not A(Brand";v="8", "Chromium";v="132", "Microsoft Edge";v="132"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }

    try:
        # Send GET request to list databases
        print("DEBUG: Sending GET request to:", url)
        print("DEBUG: Request Headers:")
        for key, value in headers.items():
            print(f"  {key}: {value}")

        response = requests.get(url, headers=headers)

        print("\nDEBUG: Response Status Code:", response.status_code)
        print("DEBUG: Response Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")

        if response.status_code == 200:
            print("\nSuccessfully connected to Stardog!")
            databases = response.json()
            print("Available Databases:", databases)
        else:
            print("\nFailed to retrieve databases. Response Text:")
            print(response.text)

    except Exception as e:
        print("Error during request:", str(e))


if __name__ == "__main__":
    # Replace with your actual Stardog endpoint and authorization header
    endpoint = "https://surround-aus.stardog.cloud:5820"
    authorization_header = "Bearer <your-access-token>"  # Replace with the actual Authorization header value

    try:
        debug_stardog_connection(endpoint, authorization_header)
    except Exception as e:
        print("Error:", e)
