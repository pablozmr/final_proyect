## Procesamiento de Datos

El procesamiento y organización de datos desempeñan un papel fundamental en este proyecto. A continuación, se presenta el esquema del flujo de procesamiento de datos respaldado por herramientas de Google Cloud.

<img src="src\proc_datos.jpeg" alt="Texto alternativo" width="700" height="400">

## Componentes del Proceso

- Data Lake
- Cloud Functions
- Data Warehouse
- Technologies

## Data Lake

La primera fase de este proyecto implica la carga de datos en bruto proporcionados y su almacenamiento en un Data Lake. ¿Pero qué es exactamente un Data Lake? Un Data Lake es un sistema de almacenamiento que permite conservar grandes volúmenes de datos en su formato original, sin necesidad de una estructura previa. Esto posibilita un acceso más rápido y flexible a los datos. Para este propósito, hemos optado por emplear Google Cloud Storage como el repositorio para almacenar los datos no procesados provenientes de fuentes como Google y Yelp.

<img src="src\Data_lake.png" alt="Texto alternativo" width="700" height="400">

## Cloud Functions

El servicio Google Cloud Functions, a través de funciones programadas, se encargará de extraer los datos del Data Lake, llevar a cabo transformaciones y limpieza, y luego cargar los datos resultantes en nuestro Data Warehouse.

<img src="src\cloud_function.png" alt="Texto alternativo" width="1200" height="400">

## Data Warehouse

¿Qué es Google BigQuery?

Google BigQuery es el servicio de Data Warehouse que utilizaremos para almacenar y estructurar nuestros datos procesados. A través de Google SQL, podremos realizar consultas personalizadas. Nuestro equipo de científicos de datos y analistas utilizará estos datos estructurados para diversos fines.

<img src="src\warehouse.png" alt="Texto alternativo" width="800" height="500">

# Tecnologías
- Google Cloud Plataform (GCP)
- Google Cloud Storage
- Google Cloud Function
- Google BigQuery
- Pandas
- Python

