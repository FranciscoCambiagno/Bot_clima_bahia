import urllib.request
import zipfile
import pandas as pd
from os import remove
from decouple import config
import requests
from bs4 import BeautifulSoup
import lxml
import re

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
    dict_tutiempo = jsonResponse["hour_hour"]["hour1"]
    
    #---Convierte el diccionario en un dataframe---
    df_tutiempo = pd.DataFrame([dict_tutiempo])
    df_tutiempo = df_tutiempo.astype('str')

    df_tutiempo.rename(columns={'hour_data':'Hora_medicion', 'temperature':'Temperatura', 'text':'Clima', 'humidity':'Humedad', 'pressure':'Presion', 'wind':'Viento'}, index={0:'Tu_Tiempo'}, inplace=True)
    df_tutiempo['Viento'] = df_tutiempo['wind_direction'] + ' ' + df_tutiempo['Viento'] + ' km/h'
    df_tutiempo['Presion'] = df_tutiempo['Presion'] + ' ' + df_tutiempo['Presion'] + ' hPa'
    df_tutiempo['Humedad'] = df_tutiempo['Humedad'] + ' ' + df_tutiempo['Humedad'] + ' %'
    df_tutiempo['Temperatura'] = df_tutiempo['Temperatura'] + ' ' + df_tutiempo['Temperatura'] + ' ºC'
    df_tutiempo.drop(columns=['date', 'icon', 'wind_direction', 'icon_wind'], inplace=True)

    #print(df_tutiempo.head(5))

    return df_tutiempo

def datos_MOTEOBAHIA():
    #---Hago el request de la pagina web y los tranformo a lxml para manipularlo---
    response = requests.get("https://www.meteobahia.com.ar/index.php")
    contenido_web = BeautifulSoup(response.text, 'lxml')

    #---Me quedo solo con los datos que me interesan bucando las etiquetas b y pl
    b = contenido_web.find_all('b')[1]
    a = contenido_web.findAll('lp')

    clima = [a[0], a[3], a[5], a[8], b]
    clima = list(map(str, clima))

    # Expresión regular para etiquetas lp, sup, b y espacion (\t)
    pattern = re.compile(r'<lp>|</lp>|<sup>|</sup>|<b>|</b>|\t')

    # Lista de strings sin etiquetas lp
    lista_sin_etiquetas = []
    for s in clima:
        new_s = pattern.sub('', s)
        lista_sin_etiquetas.append(new_s)

    lista_sin_etiquetas_spliteada = []
    for element in lista_sin_etiquetas:
        
        lista_sin_etiquetas_spliteada += element.split('\n')

    lista_sin_etiquetas_spliteada.pop(4)
    lista_sin_etiquetas_spliteada.pop(7)

    df_meteoba = pd.DataFrame([lista_sin_etiquetas_spliteada], columns=['Hora_medicion','Humedad','Presion','Radiacion','Clima','Sensacion_Termica','Viento','Temperatura'], index = ['Meteo_Bahia'])
    df_meteoba.replace(to_replace='^\s*(.*?): ', value='', regex=True, inplace=True)
    df_meteoba['Hora_medicion'] = df_meteoba['Hora_medicion'].replace(to_replace=' \s*(.*?)$', value='', regex=True)
    df_meteoba = df_meteoba[['Hora_medicion','Temperatura','Clima','Humedad','Presion','Viento','Radiacion','Sensacion_Termica']]

    return df_meteoba

def obtener_dfclima():
    path = "./"
    
    df_smn = datos_SMN(path)
    df_tutimepo = datos_TUTIEMPO()
    df_meteoba = datos_MOTEOBAHIA()

    df_clima = pd.concat([df_smn, df_tutimepo, df_meteoba])

    return df_clima

def run():
    df = obtener_dfclima()

    print(df.head())

if __name__ == "__main__":
    run()