"""
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

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos

universidad,carrera,fecha_de_inscripcion,nombre_de_usuario,sexo,fecha_nacimiento,direccion,email
UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA,LICENCIATURA_EN_CIENCIA_POLÍTICA_,15-Sep-20,WELCHMIGUEL,M,05-Oct-15,"594_ALVARADO_STREET_SUITE_945
CHANSIDE,_IL_06393",RICHARD21@HOTMAIL.COM
UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA,LICENCIATURA_EN_PLANIFICACIÓN_Y_DISEÑO_DEL_PAISAJE,20-Sep-20,RACHELFLORES,F,18-Sep-67,"9556_RYAN_FORD
PORT_DEVINBOROUGH,_UT_80704",JDALTON@HOTMAIL.COM
"""
from datetime import datetime
from pathlib import Path
import os

import pandas as pd

folder = Path(__file__).resolve().parent.parent
csvf = os.path.join(folder, 'airflow/files/ET_Univ_Villa_Maria.csv')

def get_dataframe():
    df = pd.read_csv(csvf)
    return df.head()

def lowert(col):
    return col.lower().replace('_', ' ').strip()

def process1():
    today = datetime.now()
    df = get_dataframe()
    df['universidad'] = df['universidad'].apply(lowert)
    df['carrera'] = df['carrera'].apply(lowert)
    df['email'] = df['email'].apply(lowert)
    df['sexo'] = df['sexo'].apply(lambda x: 'male' if x == 'M' else 'female')
    years = lambda x: (today - datetime.strptime(x, '%d-%b-%y')).days // 365
    df['fecha_nacimiento'] = df['fecha_nacimiento'].apply(years)
    print(df.loc[2])

