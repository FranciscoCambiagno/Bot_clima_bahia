import urllib.request
import zipfile
import pandas as pd
from os import remove
from decouple import config
import requests
from bs4 import BeautifulSoup
import lxml
import re

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

from plugins.extras import get_fechahoy

def Extract_data_SMN():
    """
    Consigue los datos del Servicio Meteorologico Nacional.
    """
  
    path_smn =  "./include/"
    path_zip = path_smn + "archivo.zip"

    #---Descarga del archivo---
    url = 'https://ssl.smn.gob.ar/dpd/zipopendata.php?dato=tiepre'
    urllib.request.urlretrieve(url, path_zip)
    print('Archivo descargado exitosamente.')  # "log" archivo descargado

    #---Extracion del zip---
    password = None

    archivo_zip = zipfile.ZipFile(path_zip, "r")
    try:
        #print(archivo_zip.namelist())
        txt_nombre = archivo_zip.namelist()[0]
        archivo_zip.extractall(pwd=password, path=path_smn)
    except:
        pass

    archivo_zip.close()

    #---Obtencion de los datos de bahia---
    df = pd.read_csv(path_smn + txt_nombre, sep=";", encoding="ISO-8859-1",
                 names = ['ciudad','fecha','Hora_medicion','Clima','Visibilidad','Temperatura','a','Humedad','Viento','Presion']
                 )

    #---Normalizacion de datos---
    df = df.astype('str')
    df['ciudad'] = df['ciudad'].replace('^ ', '', regex=True)

    bahia_smn = df.query("ciudad == 'Bahía Blanca'")    

    bahia_smn = bahia_smn[['Hora_medicion','Temperatura','Clima','Humedad','Presion','Viento','Visibilidad']]
    bahia_smn['Viento'] = bahia_smn['Viento'] + ' km/h'
    bahia_smn['Presion'] = bahia_smn['Presion'].replace(' / $', ' hPa', regex=True)
    bahia_smn['Humedad'] = bahia_smn['Humedad'] + ' ' + bahia_smn['Humedad'] + ' %'
    bahia_smn['Temperatura'] = bahia_smn['Temperatura'] + ' ' + bahia_smn['Temperatura'] + ' ºC'
    bahia_smn.rename(index={1:'SMN'}, inplace=True)
    
    #---Borrado de archivos---
    remove(path_smn + txt_nombre)
    remove(path_zip)

    #---Guarda los datos en un archivo csv---
    bahia_smn.to_csv(path_smn + f'clima{get_fechahoy()}.csv')













with DAG(
    dag_id='ETL_dag',
    description='Extrraccion, tranformacion y carga a s3 de datos del clima de la ciudad de Bahia Blanca.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 4),
    max_active_runs=5
) as dag:

    extract_from_SMN = PythonOperator(
        task_id="extract_from_SMN",
        python_callable=Extract_data_SMN
    )

    extract_from_SMN