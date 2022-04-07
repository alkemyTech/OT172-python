import pandas as pd
from datetime import datetime, date, time
from os import path
import logging


# Rutas
try:
    ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    ruta_normalized = path.abspath(path.join(ruta_base, 'lib'))
    ruta_files = path.abspath(path.join(ruta_base, 'files'))
except:
    logging.error('Error getting path')

try:
    def normalizar_universidad_de_rio_cuarto():
        """ Data normalization for University of Rio Cuarto"""

        # read csv file and create data frame
        try:
            rcuarto_df = pd.read_csv(
                f'{ruta_files}/ET_Universidad_Nacional_de_Rio_Cuarto.csv')

            # read csv file codigo poasta and create data frame
            codigo_p_df = pd.read_csv(
                f'{ruta_base}/dataset/codigos_postales.csv')
        except:
            logging.error('Error reading file csv')

        """ normalize columns: Format: lowercase, without '-', '_' or ' ' """
        rcuarto_df['university'] = rcuarto_df['university'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['career'] = rcuarto_df['career'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['first_name'] = rcuarto_df['first_name'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['last_name'] = rcuarto_df['last_name'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['location'] = rcuarto_df['location'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        rcuarto_df['email'] = rcuarto_df['email'].str.strip(
        ).str.lower()

        """ Generate female or male gender column """
        rcuarto_df['gender'] = rcuarto_df['gender'].str.replace(
            "F", "female").str.replace("M", "male")

        """ Generate location column and normalize """
        codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")
        # Create a diccionary with codigo_postales.csv
        dic = dict(
            zip(codigo_p_df['localidad'], codigo_p_df['codigo_postal']))

        def codigo(local):
            """ Generate postal_code column """
            codigo = dic[local]
            return codigo
        rcuarto_df['postal_code'] = rcuarto_df['location'].apply(codigo)

        # inscription_data str %Y-%m-%d format

        def inscription(date_inscription):
            """ Change inscription date format to yyyy-mm-dd"""
            formato = datetime.strptime(date_inscription, '%y/%b/%d')
            formato = formato.strftime('%Y-%m-%d')
            return formato
        rcuarto_df['inscription_date'] = rcuarto_df['inscription_date'].apply(
            inscription)

        # Edad

        def age(born):
            """ Get the age by your date of birth """
            born = datetime.strptime(born, "%y/%b/%d").date()
            today = date.today()
            if born.year > today.year:
                born_l = list(str(born))
                born_l[0] = '1'
                born_l[1] = '9'
                born_s = "".join(born_l)
                born = datetime.strptime(born_s, "%Y-%m-%d").date()

            return today.year - born.year - ((today.month,
                                              today.day) < (born.month, born.day))

        rcuarto_df['age'] = rcuarto_df['age'].apply(age)

        # rcuarto_df.to_excel(f'{ruta_normalized}/data1.xlsx')

        try:
            # Create .TXT file in files folder
            rcuarto_df.to_csv(
                f'{ruta_files}/ET_Universidad_Nacional_de_Rio_Cuarto.txt', index=None, sep='\t')
            logging.info('Create .txt file in files folder')
        except:
            logging.error('Error to creating .txt file')
except:
    logging.error('General error at data normalization')

