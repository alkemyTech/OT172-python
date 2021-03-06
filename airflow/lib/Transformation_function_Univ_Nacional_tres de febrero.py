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

# Data transformation function

# lines 1-3: apply the function previously defined
# line : conditional that changes m and f to male and female, in the gender column
# lines 5-6: The string contained in inscription date is passed to date format
# and the required format is assigned
# lines 7-8: Using the split function, the values ​​of the name column are separated
# and the output is split into first_name and last_name columns
# lines 9-11: The format of the birth`s date is (DD/MM/YY), when passing the
# string to date, the program did not differentiate between the decades that belonged to 1900
# and those of 2000. To obtain the age, the year was extracted from the current date, the last two characters of the date were taken,
# step to number and the age was obtained using the formula 100+(current date - date of birth)
# lines 12-16: A csv with the postal codes and their corresponding cities was passed to df.
# the names of the cities were changed to lowercase letters to match the localities in the table
# sql-query. The resulting df was passed to the dictionary, establishing the postl code variable as key (also defined in the
# sql query table. Finally, the values ​​of the zip codes in the query table are called, in the dictionary
# previously defined, resulting in the corresponding localities column).
# line 17: only the required columns were selected


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
    df['location'] = df['postal_code'].apply(lambda x: dict_postal_codes[x])

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