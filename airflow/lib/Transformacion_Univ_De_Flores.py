"""
P170-76
Una funcion que devuelva un txt para cada una de las siguientes universidades con
los datos normalizados:
Universidad De Flores

Datos Finales:
university: str minúsculas, sin espacios extras, ni guiones
career: str minúsculas, sin espacios extras, ni guiones
inscription_date: str %Y-%m-%d format
first_name: str minúscula y sin espacios, ni guiones
last_name: str minúscula y sin espacios, ni guiones
gender: str choice(male, female)
age: int
postal_code: str
location: str minúscula sin espacios extras, ni guiones
email: str minúsculas, sin espacios extras, ni guiones

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos

universidad, carrera, fecha_de_inscripcion, nombre_de_usuario, sexo, fecha_nacimiento,
codigo_postal, direccion, correo_electronico
"""

from datetime import datetime
from pathlib import Path
import os

import pandas as pd
import numpy as np

folder = Path(__file__).resolve().parent.parent
csv_file = os.path.join(folder, 'files/ET_Univ_De_Flores.csv')
list1 = ['universidad', 'carrera', 'fecha_de_inscripcion', 'sexo', 'fecha_nacimiento', 'codigo_postal', 'direccion', 'correo_electronico']
list2 = ['university', 'career', 'inscription_date', 'gender', 'age', 'postal_code', 'location', 'email']
diccionario = dict(zip(list1, list2))

def get_dataframe():
    df = pd.read_csv(csv_file)
    return df

def lower_trans(col):
    return col.lower().replace('_', ' ').replace('\n', ' ').strip()

def process_df():
    df = get_dataframe()
    today = datetime.now()
    df['universidad'] = df['universidad'].apply(lower_trans)
    df['carrera'] = df['carrera'].apply(lower_trans)
    df['email'] = df['email'].apply(lower_trans)
    df['direccion'] = df['direccion'].apply(lower_trans)
    df['sexo'] = df['sexo'].apply(lambda x: 'male' if x == 'M' else 'female')
    get_age = lambda x: (today - datetime.strptime(x, '%Y-%m-%d')).days // 365
    df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(get_age)
    df.drop(columns=['nombre_de_usuario'], inplace=True)
    df.rename(columns=diccionario, inplace=True)
    df['first_name'] = np.nan
    df['last_name'] = np.nan
    df['postal_code'] = np.nan
    df.to_csv(os.path.join(folder, 'files/ET_Univ_De_Flores.txt'), sep='\t', index=None)

if __name__ == '__main__':
    process_df()
