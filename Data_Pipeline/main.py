import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery
from textblob import TextBlob
import re
import functions_framework
from datetime import datetime
import numpy as np

client = bigquery.Client()
print("Cliente iniciado correctamente")

'''
Python Dependencies to be installed

gcsfs
fsspec
pandas
pandas-gbq

'''

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    lst = []
    # Extraer el nombre del evento, incluido el nombre de todas las carpetas.
    file_name = event['name']
    # Extraer el tipo del archivo.
    file_type = file_name.split('.')[-1]
    # Extraer el nombre del archivo
    if "/" in file_name:
        main_folder = file_name.split("/")[0]
        last_folder = file_name.split("/")[file_name.count("/")-1]
        print(last_folder)
        if main_folder == "Google-Maps":
            if file_name.split("/")[1] == "reviews-estados":
                table_name = "Reviews"
                state = last_folder.split("-")[-1]
                dataset = main_folder.split("-")[0] + "."
    
            if file_name.split("/")[1] == "metadata-sitios":
                table_name = "Metadata"
                dataset = main_folder.split("-")[0] + "."
    
        if main_folder == "Yelp":
            table_name = file_name.split(".")[0].split("/")[1]
            
            dataset = "yelp."
            if "review" in table_name:
                table_name = "review"
        if main_folder == "Details-Api":
            table_name = file_name.split(".")[0].split("/")[1]
            dataset = "new_sources."
    else:
        table_name = file_name.split('.')[0]
        dataset = "new_sources."

    # Evento, detalles de metadatos del archivo que se escriben en Big Query
    dct={
         'Event_ID':context.event_id,
         'Event_type':context.event_type,
         'Bucket_name':event['bucket'],
         'File_name':event['name'],
         'Created':event['timeCreated'],
         'Updated':event['updated']
        }
    lst.append(dct)
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq(dataset + 'data_loading_metadata', 
                        project_id='watchful-pier-394700', 
                        if_exists='append',
                        location='us')
    
    # Compruebe si el tipo de archivo es csv
    if file_type == "csv":

        df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name)

    # Compruebe si el tipo de archivo es json  
    if file_type == "json":
        
        try:
            # Carga del archivo json
            df_data = pd.read_json('gs://' + event['bucket'] + '/' + file_name)
        except ValueError as e:
            if "Trailing data" in str(e):
                # Ejecute esta línea de código alternativa
                df_data = pd.read_json('gs://' + event['bucket'] + '/' + file_name, lines = True)
            else:
                # Gestionar otro tipo de errores
                print("An error occurred while loading the JSON file:", e)
        
    # Compruebe si el tipo de archivo es parquet
    if file_type == "parquet":
        
        df_data = pd.read_parquet('gs://' + event['bucket'] + '/' + file_name)
    
    # Compruebe si el tipo de archivo es pkl
    if file_type == "pkl":

        df_data = pd.read_pickle('gs://' + event['bucket'] + '/' + file_name)
        print("Lectura de archivo correcta")
    
    # -= Transformaciones =-
    # Transformaciones Google
    if main_folder == "Google-Maps":

        if last_folder == "reviews-estados":
            # Ahora la columna 'time' contendrá objetos de tipo datetime
            df_data.time = df_data.time.apply(unix_to_datetime)
            df_data[['resp_time', 'resp_text']] = df_data['resp'].apply(separar_llaves).apply(pd.Series)

            df_data.resp_time = df_data.resp_time.apply(unix_to_datetime)
            # Calcular la diferencia de tiempo y crear una nueva columna 'diferencia_tiempo'
            df_data['diferencia_tiempo'] = df_data.apply(lambda row: row['resp_time'] - row['time'] if not pd.isna(row['resp_time']) else pd.NaT, axis=1)
            #Creamos la columna "Feeling" que define la apreciación sobre el mismo(sentimiento) de la columna texto 
            df_data['feeling'] = df_data['text'].apply(lambda x: classify_comment2(x) if pd.notnull(x) else 'No message')
            df_data['feeling_resp'] = df_data['resp_text'].apply(lambda x: classify_comment2(x) if pd.notnull(x) else 'No message')

            

            print("Transformaciones correctas")

            table_ref = client.dataset("Google").table(table_name)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_data, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta")

        if last_folder == "metadata-sitios":
            df_data["platform"] = "google"
            # Diccionario de códigos postales y estados de Estados Unidos (sin tildes)
            us_states_dict = {
                'AL': 'Alabama',
                'AK': 'Alaska',
                'AZ': 'Arizona',
                'AR': 'Arkansas',
                'CA': 'California',
                'CO': 'Colorado',
                'CT': 'Connecticut',
                'DE': 'Delaware',
                'FL': 'Florida',
                'GA': 'Georgia',
                'HI': 'Hawaii',
                'ID': 'Idaho',
                'IL': 'Illinois',
                'IN': 'Indiana',
                'IA': 'Iowa',
                'KS': 'Kansas',
                'KY': 'Kentucky',
                'LA': 'Louisiana',
                'ME': 'Maine',
                'MD': 'Maryland',
                'MA': 'Massachusetts',
                'MI': 'Michigan',
                'MN': 'Minnesota',
                'MS': 'Mississippi',
                'MO': 'Missouri',
                'MT': 'Montana',
                'NE': 'Nebraska',
                'NV': 'Nevada',
                'NH': 'New Hampshire',
                'NJ': 'New Jersey',
                'NM': 'New Mexico',
                'NY': 'New York',
                'NC': 'North Carolina',
                'ND': 'North Dakota',
                'OH': 'Ohio',
                'OK': 'Oklahoma',
                'OR': 'Oregon',
                'PA': 'Pennsylvania',
                'RI': 'Rhode Island',
                'SC': 'South Carolina',
                'SD': 'South Dakota',
                'TN': 'Tennessee',
                'TX': 'Texas',
                'UT': 'Utah',
                'VT': 'Vermont',
                'VA': 'Virginia',
                'WA': 'Washington',
                'WV': 'West Virginia',
                'WI': 'Wisconsin',
                'WY': 'Wyoming'
            }



            df_data.address.fillna("sin datos", inplace=True)
            # Aplicar la función a cada fila del DataFrame
            df_data['address'] = df_data.apply(eliminar_valores_duplicados, axis=1)

            df_cp = pd.DataFrame({"cp": df_data['address'].str.extract(r'([A-Z]{2}\s?\d{5}(?:-\d{4})?)').squeeze()})
            df_cp['states'] = df_cp['cp'].str.extract(r'([A-Z]{2})')


            df_data['address'] = df_data['address'].str.replace(r'[A-Z]{2}\s?\d{5}(?:-\d{4})?', '').str.strip()
            # Aplicar la función a la columna address para extraer la ciudad
            df_city = pd.DataFrame({"city": df_data['address'].apply(extraer_ciudad)})

            # Aplicar la función a la columna address
            df_data['address'] = df_data['address'].apply(eliminar_ciudades)

            df_location = pd.concat([df_data['gmap_id'], df_data['address'], df_city, df_cp['states'], df_cp['cp'], df_data['latitude'], df_data['longitude']], axis=1)
            df_location.columns = ['gmap_id', 'address', 'city', 'state', 'cp', 'latitude', 'longitude']


            df_data.drop(columns=['address', 'latitude', 'longitude'], inplace=True)

            df_data.category = df_data.category.apply(list_to_string)
            df_data.relative_results = df_data.relative_results.apply(list_to_string)

            df_data.description.fillna("sin datos", inplace=True)
            df_data.price.fillna("sin datos", inplace=True)

            df_data.MISC = df_data.MISC.apply(replace_nan_with_dict)
            # Paso 1: Asegúrate de que la columna 'MISC' sea del tipo dict (puede que no necesites esta línea si ya está en el formato correcto)
            df_data['MISC'] = df_data['MISC'].apply(lambda x: {} if x is None else x)

            # Paso 2: Encontrar todas las llaves únicas presentes en los diccionarios
            all_keys = set()
            for dictionary in df_data['MISC']:
                all_keys.update(dictionary.keys())

            # Paso 3 y 4: Iterar sobre la columna y crear un nuevo DataFrame
            new_data = []
            for dictionary in df_data['MISC']:
                row_data = {}
                for key in all_keys:
                    row_data[key] = dictionary.get(key, None)
                new_data.append(row_data)

            df_misc = pd.DataFrame(new_data)

            # Ahora tienes un nuevo DataFrame "new_df" con las llaves como columnas y valores nulos donde no se encontraron las llaves.

            column_misc = df_misc.columns.to_list()
            for column in column_misc:
                df_misc[column] = df_misc[column].apply(list_to_string)
                
                
            # Procesar la columna 'hours' del DataFrame df_hours
            df_data['hours'] = df_data['hours'].apply(process_hours)

            # Construir un nuevo DataFrame df_horarios a partir de los datos procesados
            df_horarios = pd.DataFrame(df_data['hours'].tolist())
            df_horarios.fillna('sin datos', inplace= True)
            df_horarios['gmap_id'] = df_data.gmap_id    
            df_misc.fillna('sin datos', inplace= True)    
            df_misc['gmap_id'] = df_data.gmap_id    
            df_data.drop(columns = ['hours', 'MISC'], inplace= True)
            df_data.category.fillna("sin datos", inplace=True)
            df_data.relative_results.fillna("sin datos", inplace=True)
            df_data.state.fillna('sin datos', inplace=True)
            df_data = df_data.drop_duplicates().reset_index(drop=True)
            df_location = df_location.drop_duplicates().reset_index(drop=True)
            df_horarios = df_horarios.drop_duplicates().reset_index(drop=True)
            df_misc = df_misc.drop_duplicates().reset_index(drop=True)

            df1 = df_data[df_data['category'].str.contains('Hotel', na=False, case=True)]
            gmap_hotel = df1['gmap_id']
            df2 = df_location[df_location['state'].str.contains('CA|NY', na=False, case=True)]
            df3 = df_location[df_location['city'].str.contains('Las Vegas', na=False, case=True)]

            df = pd.concat([df2, df3])
            gmap_ids_metadata = df['gmap_id']
            gmap_hotel = gmap_ids_metadata[gmap_ids_metadata.isin(gmap_hotel)]
            

            df_location = df_location[df_location['gmap_id'].isin(gmap_hotel)].reset_index(drop=True)
            df_horarios = df_horarios[df_horarios['gmap_id'].isin(gmap_hotel)].reset_index(drop=True)
            df_misc = df_misc[df_misc['gmap_id'].isin(gmap_hotel)].reset_index(drop=True)
            df_data = df_data[df_data['gmap_id'].isin(gmap_hotel)].reset_index(drop=True)

            df_location.replace(us_states_dict, inplace=True)

            print("Transformacion ejecutada correctamente")

            table_ref = client.dataset("Google").table(table_name)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_data, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta")

            table_ref = client.dataset("Google").table('locacion')
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_location, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta locacion")

            table_ref = client.dataset("Google").table('miscelaneas')
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_misc, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta miscelaneas")

            table_ref = client.dataset("Google").table('horarios')
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_horarios, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta horarios")

    #--------------- Yelp Transformations -----------
    if main_folder == "Yelp":
        
        if table_name == "business":
            
            print("Transformacones ejecutadas correctamente")

            table_ref = client.dataset("yelp").table(table_name)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_data, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta")

        if table_name == "user":
            df_data.drop(columns=["Unnamed: 0","name","yelping_since","funny","cool","elite","fans","average_stars","compliment_hot","compliment_more","compliment_profile","compliment_cute","compliment_list","compliment_note","compliment_plain","compliment_cool","compliment_funny","compliment_writer","compliment_photos"],inplace=True)
            df_data.rename(columns={"review_count":"num_of_reviews"}, inplace=True)
            print("Transformaciones ejecutadas correctamente")
        
        if table_name == "tip" or table_name.startswith("tip_"):
            df_data.rename(columns={"text":"opinion"}, inplace=True)
            df_data['feeling'] = df_data['opinion'].apply(lambda x: classify_comment2(x) if pd.notnull(x) else 'No message')
            print("Transformaciones ejecutadas correctamente")

            table_ref = client.dataset("yelp").table(table_name)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job = client.load_table_from_dataframe(df_data, table_ref, job_config=job_config)
            job.result()
            print("Carga correcta")
        
        df_data.drop_duplicates(inplace=True)



   

# Function to classify comments
def classify_comment2(comment):
    if comment is None:
        return 'No message'
    else:
        sentiment = TextBlob(comment).sentiment.polarity
        if sentiment > 0:
            return 'Positive'
        elif sentiment < 0:
            return 'Negative'
        else:
            return 'Neutral'


def unix_to_datetime(unix_timestamp):
    if pd.notna(unix_timestamp):  # Verificar si el valor no es NaN
        return datetime.fromtimestamp(unix_timestamp / 1000)  # Dividimos por 1000 para obtener los segundos
    else:
        return np.nan  # Devolver NaN para los valores NaN

# Función para separar las llaves 'time' y 'text' en dos columnas y rellenar con {'sin datos': 'sin datos'} cuando no existan esas llaves
def separar_llaves(diccionario):
    if diccionario is None:
        return {'time': np.NaN, 'text': None}

    else:
        return {'time': diccionario.get('time', np.NaN), 'text': diccionario.get('text', None)}

# Función para eliminar valores de la columna address que existen en la columna name
def eliminar_valores_duplicados(row):
    address = row['address']
    name = row['name']

    if isinstance(address, str) and isinstance(name, str):
        return address.replace(name, '').strip()



def extraer_ciudad(cadena):
    if cadena is None:
        return 'sin datos'

    ciudades = cadena.split(',')
    if len(ciudades) > 2:
        return ciudades[-2].strip()
    else:
        return 'sin datos'



def eliminar_ciudades(cadena):
    if cadena is None:
        return 'sin datos'
        
    patron_ciudades = r',\s*([A-Za-z\s-]+),?\s*'
    return re.sub(patron_ciudades, '', cadena)

# Función para convertir una lista en una cadena separada por comas
def list_to_string(lst):
    if isinstance(lst, list):
        return ', '.join(map(str, lst))
    return lst

# Función para reemplazar valores NaN por el diccionario {"sin datos": ["sin datos"]}
def replace_nan_with_dict(value):
    return value if isinstance(value, dict) else {"sin datos": ["sin datos"]}

# Función para convertir la lista de horarios en un diccionario con los días como clave y los horarios como valor
def process_hours(hours_list):
    if hours_list is None:
        return {}

    hours_dict = {}
    for day, hours in hours_list:
        hours_dict[day] = hours

    return hours_dict

def eliminar_cp(cadena):
    patron_cp = r'([A-Z]{2}\s?\d{5}(?:-\d{4})?)'
    return re.sub(patron_cp, '', cadena)