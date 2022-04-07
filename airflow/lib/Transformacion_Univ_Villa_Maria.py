"""
PT170-76
Una funcion que devuelva un txt para cada una de las siguientes universidades con los datos normalizados:
Universidad Nacional De Villa María

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

university, career, inscription_date, first_name, last_name, gender, age, postal_code, location, email

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos
"""
from datetime import datetime
from pathlib import Path
import os

import pandas as pd
import numpy as np

folder = Path(__file__).resolve().parent.parent
csv_file = os.path.join(folder, 'files/ET_Univ_Villa_Maria.csv')
list1 = ['universidad', 'carrera', 'fecha_de_inscripcion', 'sexo', 'fecha_nacimiento', 'direccion', 'email']
list2 = ['university', 'career', 'inscription_date', 'gender', 'age', 'location', 'email']
diccionario = dict(zip(list1, list2))

def get_dataframe():
    df = pd.read_csv(csv_file)
    return df

def str_trans(col):
    return col.lower().replace('_', ' ').replace('\n', ' ').strip()

def format_date(x):
    p1 = datetime.strptime(x, '%d-%b-%y')
    return datetime.strftime(p1, '%Y-%m-%d')

def process_df():
    df = get_dataframe()
    today = datetime.now()
    df['universidad'] = df['universidad'].apply(str_trans)
    df['carrera'] = df['carrera'].apply(str_trans)
    df['email'] = df['email'].apply(str_trans)
    df['direccion'] = df['direccion'].apply(str_trans)
    df['fecha_de_inscripcion'] = df['fecha_de_inscripcion'].apply(format_date)
    df['sexo'] = df['sexo'].apply(lambda x: 'male' if x == 'M' else 'female')
    get_age = lambda x: (today - datetime.strptime(x, '%d-%b-%y')).days // 365
    df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(get_age)
    df.rename(columns=diccionario, inplace=True)
    df.drop(columns=['nombre_de_usuario'], inplace=True)
    df['first_name'] = np.nan
    df['last_name'] = np.nan
    df['postal_code'] = np.nan
    df.to_csv(os.path.join(folder, 'files/ET_Univ_Villa_Maria.txt'), sep='\t', index=None)

if __name__ == '__main__':
    process_df()
