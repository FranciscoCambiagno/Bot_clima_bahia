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
    print('Archivo descargado exitosamente.')

    #---Extracion del zip---
    password = None

    archivo_zip = zipfile.ZipFile(path_zip, "r")
    try:
        print(archivo_zip.namelist())
        txt_nombre = archivo_zip.namelist()[0]
        archivo_zip.extractall(pwd=password, path=path_smn)
    except:
        pass

    archivo_zip.close()

    #---Obtencion de los datos de bahia---
    df = pd.read_csv(path_smn + txt_nombre, sep=";", encoding="ISO-8859-1",
                 names = ['ciudad','fecha','hora_regist','clima','visibilidad','humedad_%','a','temperatura','viento','presion']
                 )

    df['ciudad'] = df['ciudad'].replace('^ ', '', regex=True)

    bahia_smn = df.query("ciudad == 'Bahía Blanca'")
    bahia_smn = bahia_smn.values.tolist()
    print(bahia_smn)    # Print de el clima en bahia
    remove(path_smn + txt_nombre)
    remove(path_zip)

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
    print(dict_tutiempo)
    return(dict_tutiempo)


def run():
    path = "E:/Programacion/Proyetos/Clima/"
    
    datos_SMN(path)
    datos_TUTIEMPO()

if __name__ == "__main__":
    run()