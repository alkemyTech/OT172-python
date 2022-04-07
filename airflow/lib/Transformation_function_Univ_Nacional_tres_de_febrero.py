import pandas as pd
import logging 
import os
import pathlib
import datetime
from datetime import datetime

# To define the directory, the pathlib.Path(__file__) function of the payhlib module was used.
#  This function detects the path of the running .py file. Since that file is in /dags, it is
#  necessary to move up one level. This is achieved with the .parent method.

def normalize_characters(column):
    column = column.apply(lambda x: str(
        x).replace(' \W'+'*'+'\W', '\W'+'*'+'\W'))
    column = column.apply(lambda x: str(
        x).replace('\W'+'*'+'\W ', '\W'+'*'+'\W'))
    column = column.apply(lambda x: str(x).replace('-', ' '))
    column = column.apply(lambda x: str(x).replace('_', ' '))
    column = column.apply(lambda x: x.lower())
    return column



def transformation(df):
    path=(pathlib.Path(__file__).parent.absolute()).parent
    logging.info(f'normalizing data')
    df['university'] = normalize_characters(df['university'])
    df['career'] = normalize_characters(df['career'])
    df['gender'] = df['gender'].apply(
        lambda x: 'male' if x == 'm' else 'female')

    old_date = pd.to_datetime(df['inscription_date'])
    df['inscription_date'] = pd.to_datetime(old_date, '%Y/%m/%d')

    df['first_name'] = df['name'].apply(lambda x: str(x).split('_')[0])
    df['last_name'] = df['name'].apply(lambda x: str(x).split('_')[1])

    curr = datetime.now()
    df['age'] = df['nacimiento'].apply(lambda x: (
        100+(int(str(curr.year)[2:4]) - int(x[7:9]))))

    df_postal_codes = (pd.read_csv(f'{path}/dataset/codigos_postales.csv'))
    df_postal_codes['localidad'] = df_postal_codes['localidad'].apply(
        lambda x: x.lower())
    dict_postal_codes = dict(
        zip(df_postal_codes.codigo_postal, df_postal_codes.localidad))
    df['location'] = df['postal_code'].apply(lambda x: dict_postal_codes[int(x)])

    df = df[['university', 'career', 'inscription_date', 'first_name',
             'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    df.to_csv(f'{path}/files/ET_Univ_nacional_tres_de_febrero.txt', sep='\t')
    return(df)


def main():
    path_input = (pathlib.Path(__file__).parent.absolute()).parent
    path= f'{path_input}/files/Extraction_Univ_nacional_tres_de_febrero.csv'
    df= pd.read_csv(path)
    transformation(df)

if __name__=='__main__':
    main()