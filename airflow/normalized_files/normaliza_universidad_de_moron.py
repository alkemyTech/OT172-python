import pandas as pd
from datetime import datetime, date
from os import path
import logging


# Rutas
ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
ruta_normalized = path.abspath(path.join(ruta_base, 'normalized_files'))
ruta_files = path.abspath(path.join(ruta_base, 'files'))


def normalizar_universidad_de_moron():
    """ Data normalization for University of Moron """

    # read csv file and create data frame
    moron_df = pd.read_csv(f'{ruta_files}/ET_Universidad_de_Moron.csv')
    # read csv file codigo poasta and create data frame
    codigo_p_df = pd.read_csv(f'{ruta_base}/dataset/codigos_postales.csv')

    # Format: lowercase, without '-', '_' or ' '
    moron_df['university'] = moron_df['university'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['career'] = moron_df['career'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['first_name'] = moron_df['first_name'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['last_name'] = moron_df['last_name'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    moron_df['email'] = moron_df['email'].str.strip(
    ).str.lower()

    # Gender to male o Female
    moron_df['gender'] = moron_df['gender'].str.replace(
        "F", "female").str.replace("M", "male")

    # Generate location column and normalize

    codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
    ).str.lower().str.replace("-", " ").str.replace("_", " ")
    dic = dict(
        zip(codigo_p_df['codigo_postal'], codigo_p_df['localidad']))

    def location(codigo):
        location = dic[codigo]
        return location
    moron_df['location'] = moron_df['postal_code'].apply(location)

    # inscription_data str %Y-%m-%d format

    def inscription(date_inscription):
        """ Change inscription date format to yyyy-mm-dd"""
        formato = datetime.strptime(date_inscription, '%d/%m/%Y')
        formato = formato.strftime('%Y-%m-%d')
        return formato
    moron_df['inscription_date'] = moron_df['inscription_date'].apply(
        inscription)

    # Edad
    def age(born):
        """ Get the age by your date of birth """
        born = datetime.strptime(born, "%d/%m/%Y").date()
        today = date.today()
        return today.year - born.year - ((today.month,
                                          today.day) < (born.month, born.day))
    moron_df['age'] = moron_df['age'].apply(age)

    # moron_df.to_excel(f'{ruta_normalized}/data1.xlsx')

    # Create .TXT file in files folder
    moron_df.to_csv(
        f'{ruta_files}/ET_Universidad_de_Moron.txt', index=None, sep='\t')
