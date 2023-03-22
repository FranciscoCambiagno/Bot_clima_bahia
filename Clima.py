import urllib.request
import zipfile
import pandas as pd
from os import remove

def datos_SMN(path):
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



def run():
    path = "E:/Programacion/Proyetos/Clima/"
    
    datos_SMN(path)

if __name__ == "__main__":
    run()