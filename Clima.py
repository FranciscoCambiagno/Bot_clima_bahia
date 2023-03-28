import urllib.request
import zipfile
import pandas as pd
from os import remove
from decouple import config
import requests

def datos_SMN(path):
    """
    Consigue los datos del Servicio Meteorologico Nacional.
    """
  
    path_smn = path + "SMN/"
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
    df['ciudad'] = df['ciudad'].replace('^ ', '', regex=True)

    bahia_smn = df.query("ciudad == 'Bahía Blanca'")

    bahia_smn = bahia_smn[['Hora_medicion','Temperatura','Clima','Humedad','Presion','Viento','Visibilidad']]
    bahia_smn['Viento'] = bahia_smn['Viento'] + ' km/h'
    bahia_smn.rename(index={1:'SMN'}, inplace=True)
    
    #---Borrado de archivos---
    remove(path_smn + txt_nombre)
    remove(path_zip)

    #print(bahia_smn.head(5))
    return bahia_smn

def datos_TUTIEMPO():
    """
    Consigue los datos de la pagina Tu Tiempo.
    """

    dict_tutiempo = {}

    #---Extrae la informacion en un json---
    key = config('KEY_TUTIEMPO')
    response = requests.get(f'https://api.tutiempo.net/json/?lan=es&apid={key}&lid=42815')
    jsonResponse = response.json()
    
    #---Saca del json los datos del cima actuales--- 
    dict_tutiempo  = jsonResponse["hour_hour"]["hour1"]
    
    #---Convierte el diccionario en un dataframe---
    df_tutiempo = pd.DataFrame([dict_tutiempo])
    df_tutiempo = df_tutiempo.astype('str')

    df_tutiempo.rename(columns={'hour_data':'Hora_medicion', 'temperature':'Temperatura', 'text':'Clima', 'humidity':'Humedad', 'pressure':'Presion', 'wind':'Viento'}, index={0:'Tu_Tiempo'}, inplace=True)
    df_tutiempo['Viento'] = df_tutiempo['wind_direction'] + ' ' + df_tutiempo['Viento'] + ' km/h'
    df_tutiempo.drop(columns=['date', 'icon', 'wind_direction', 'icon_wind'], inplace=True)

    #print(df_tutiempo.head(5))

    return df_tutiempo

def obtener_dfclima():
    path = "E:/Programacion/Proyetos/Clima/"
    
    df_smn = datos_SMN(path)
    df_tutimepo = datos_TUTIEMPO()

    df_clima = pd.concat([df_smn, df_tutimepo])

    return df_clima

def run():
    df = obtener_dfclima()

    print(df.head())

if __name__ == "__main__":
    run()