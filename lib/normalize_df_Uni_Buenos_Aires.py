import pandas as pd
import numpy as np
import pathlib
from datetime import datetime
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher as SM


""" Input: Ruta + nombre del Archivo.csv a normalizar(ET_Univ_Buenos_Aires.csv)
           Ruta + nombre del Archivo2.csv con los postal_code y locations
           Ruta de la carpeta donde se guardara el .csv en formato .txt
    Output: .txt en la ruta del tercer parametro
"""
def normalize_df_Uni_Buenos_Aires(path_df, path_dfmerge, path_download):
  ### Lectura y renombre del csv a normalizar ###
  
  dateparse = lambda x: datetime.strptime(x, '%y-%b-%d')
  df = pd.read_csv(path_df, parse_dates=['fechas_nacimiento',
                                         'fechas_de_inscripcion'],
                            date_parser = dateparse)

  df.columns = ['university', 'career', 'inscription_date', 'full_name',
                'gender', 'birth_date', 'postal_code', 'email']
  
  ### Lectura y renombre del csv con postal_code y location ###
  dfmerge = pd.read_csv(path_dfmerge)
  dfmerge.columns = ['postal_code', 'location']
  dfmerge['location'] = dfmerge['location'].apply(lambda x: x.lower())
  
  ### Creando columnas con valores nulos por defecto
  df['first_name'] = np.nan
  df['last_name'] = np.nan
  
  ### Recorrer todas las filas y normalizar fila por fila, columna por columna
  for row_i in range(len(df)):
    ##### University
    uni = df.loc[row_i, 'university']
    df.loc[row_i, 'university'] = uni.lower().replace("-"," ").strip()

    ##### Career
    carr = df.loc[row_i, 'career']
    df.loc[row_i, 'career'] = carr.lower().replace("-"," ").strip()

    #### inscription_date
    ins_date = df.loc[row_i, 'inscription_date']
    df.loc[row_i, 'inscription_date'] =  ins_date.strftime('%Y-%m-%d')

    #### fist name
    # lista de abreviaciones mas usadas
    list_abrev = ['dr', 'dra', 'ms', 'mrs', 'ing', 'lic', 'ph.d', 'mtro', 'arq']
    # funcion auxiliar, comprueba que si existe alguna abreviacion de profesion
    def match_check(elem, list_c):
      for i in list_c:
        return True if SM(None,elem.lower(),i).ratio() > 0.6 else ""
      return False

    fname = df.loc[row_i, 'full_name'].lower().split("-")
    fname = fname[1:] if match_check(fname[0],list_abrev) else fname
    df.loc[row_i, 'first_name'] = fname[0]

    #### last name
    df.loc[row_i, 'last_name'] = fname[-1]

    #### gender
    gen = df.loc[row_i, 'gender']
    df.loc[row_i, 'gender'] = ((np.nan,'female')[gen == 'f'],'male')[gen == 'm']

    #### age
    age = df.loc[row_i, 'birth_date']
    df.loc[row_i, 'birth_date'] = relativedelta(datetime.now(), age).years

    #### email, no pueden tener espacios pero si guiones entre los caracteres
    email = df.loc[row_i, 'email']
    df.loc[row_i, 'email'] = email.lower().strip("-").replace(" ","")

  #### location, realizaremos un left join
  df = df.merge(dfmerge, on='postal_code', how='left')

  # eliminar y renombrar columnas
  df = df.drop(['full_name'], axis=1).rename(columns={'birth_date':'age'})
  
  #importar a una ruta especifica
  df.to_csv(path_download + 'Uni_Buenos_Aires.txt', sep= "|")
  


if __name__ == '__main__':
    pathOne = str(pathlib.Path().absolute()) + '/../files/'
    pathTwo = str(pathlib.Path().absolute()) + '/../dataset/'
    normalize_df_Uni_Buenos_Aires(pathOne + 'ET_Univ_Buenos_Aires.csv',
            pathTwo + 'codigos_postales.csv',
            pathOne)
