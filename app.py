from azure.storage.blob import BlobServiceClient
from flask import Flask
from io import StringIO
import csv

# Define the connection string and the container name
conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
container_name = 'prova'


app = Flask(__name__)


# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"

# Creación de un fichero en blob storage
@app.route('/calcul_enun', methods=['GET'])
def calcul_enun():

    # Create a BlobServiceClient object using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Create a new blob (file) in the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='python.csv')

    # Define the data to be written to the CSV file
    data = [
        ['Name', 'Age', 'Gender'],
        ['John', 25, 'Male'],
        ['Jane', 30, 'Female'],
        ['Bob', 40, 'Male']
    ]

    # Write the data to a string buffer
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for row in data:
        writer.writerow(row)

    # Convert the string buffer to a bytes object
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    # Upload the bytes object to the blob
    blob_client.upload_blob(csv_bytes, overwrite=True)
    
    print('CSV file created successfully!')

    return 'CSV file created successfully!'


# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()
