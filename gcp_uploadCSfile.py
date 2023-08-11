# import packages
from google.cloud import storage
import os

# establecer la ruta del archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'ServiceKey_GoogleCloud.json'

# Definimos la funcion para agregar archivos al bucket
def upload_cs_file(bucket_name, source_file_name, destination_file_name): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    # blob.upload_from_filename(source_file_name)

    if blob.exists():
        print(f"El archivo {destination_file_name} ya existe en el bucket.")
        return False

    blob.upload_from_filename(source_file_name)
    print("Archivos cargados con exito")
    
    return True


upload_cs_file('jsj_data_bucket', 'Google/reseñas.csv', 'Google/reseñas')
upload_cs_file('jsj_data_bucket', 'metadata/horarios/horario.csv', 'Google/horario')
upload_cs_file('jsj_data_bucket', 'metadata/metadata.csv', 'Google/misc')
upload_cs_file('jsj_data_bucket', 'metadata/metadata.csv', 'Google/metadata')
upload_cs_file('jsj_data_bucket', 'Yelp/business_california.csv', 'Yelp/business_california')
upload_cs_file('jsj_data_bucket', 'Yelp/checkin_california.csv', 'Yelp/checkin_california')
upload_cs_file('jsj_data_bucket', 'Yelp/reviews_california.csv', 'Yelp/reviews_california')
