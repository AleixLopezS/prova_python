# --- LLIBRERIES ---
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.baseblobservice import BaseBlobService
from azure.storage.blob import BlobPermissions

from flask import Flask
from io import StringIO
import os
import csv

import pandas as pd
import numpy as np
import json
from datetime import datetime
import solver

account_name = 'stfwehqdcdes'
account_key = 'gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ=='
container_name = 'prova'
blob_fichero_any = 'LK_ANY.csv'


blob_service = BaseBlobService(
    account_name=account_name,
    account_key=account_key
)

# Define the connection string and the container name
conn_str = os.environ['CUSTOMCONNSTR_blobstorage']
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
    
    blob_url_fich = blob_service.make_blob_url(container_name, blob_fichero_any)
    
    df = pd.read_csv(blob_url_fich)

    # Create a new blob (file) in the container
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='salida.csv')

    # Define the data to be written to the CSV file
    data = [
        ['2023'],
        ['2024'],        
        ['2025']
    ]
    
    df.append(data)
    
    # Write the data to a string buffer
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for row in df:
        writer.writerow(row)

    # Convert the string buffer to a bytes object
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    # Upload the bytes object to the blob
    blob_client.upload_blob(csv_bytes, overwrite=True)
    
    print('CSV file created successfully! hey')

    return 'CSV file created successfully!'


# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()