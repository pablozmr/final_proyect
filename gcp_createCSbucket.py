from google.cloud import storage
import os

# establecer la ruta del archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'finalproject\ServiceKey_GoogleCloud.json'

# Definimos la funcion para crear el bucket
def create_bucket(bucket_name, storage_class='STANDARD', location='southamerica-east1'): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class
   
    bucket = storage_client.create_bucket(bucket, location=location) 
    
    return f'Bucket {bucket.name} successfully created.'

## Invocamos la funcion
print(create_bucket('jsj_data_bucket', 'STANDARD', 'us-central1'))