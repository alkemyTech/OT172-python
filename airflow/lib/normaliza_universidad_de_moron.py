import pandas as pd
from datetime import datetime, date
from os import path
import logging

try:
    # Rutas
    ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
    ruta_normalized = path.abspath(path.join(ruta_base, 'lib'))
    ruta_files = path.abspath(path.join(ruta_base, 'files'))
except:
    logging.error('Error getting path')

try:
    def normalizar_universidad_de_moron():
        """ Data normalization for University of Moron """

        logging.info('Start de process function')
        try:
            # read csv file and create data frame
            moron_df = pd.read_csv(f'{ruta_files}/ET_Universidad_de_Moron.csv')
            # read csv file codigo poasta and create data frame
            codigo_p_df = pd.read_csv(
                f'{ruta_base}/dataset/codigos_postales.csv')
            logging.info('Data frame created')
        except:
            logging.error('Error reading file csv ')

        """ normalize columns: Format: lowercase, without '-', '_' or ' ' """
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

        """ Gender to male o Female """
        moron_df['gender'] = moron_df['gender'].str.replace(
            "F", "female").str.replace("M", "male")

        """Generate location column and normalize"""
        codigo_p_df['localidad'] = codigo_p_df['localidad'].str.strip(
        ).str.lower().str.replace("-", " ").str.replace("_", " ")

        dic = dict(
            zip(codigo_p_df['codigo_postal'], codigo_p_df['localidad']))

        def location(codigo):
            """ Create location column """
            location = dic[codigo]
            return location
        moron_df['location'] = moron_df['postal_code'].apply(location)
        logging.info('column location created and normalized')


        # inscription_data str %Y-%m-%d format
        def inscription(date_inscription):
            """ Change inscription date format to yyyy-mm-dd"""
            formato = datetime.strptime(date_inscription, '%d/%m/%Y')
            formato = formato.strftime('%Y-%m-%d')
            return formato
        moron_df['inscription_date'] = moron_df['inscription_date'].apply(
            inscription)
        logging.info('Change inscription date format')


        # Edad
        def age(born):
            """ Get the age by your date of birth """
            born = datetime.strptime(born, "%d/%m/%Y").date()
            today = date.today()
            return today.year - born.year - ((today.month,
                                              today.day) < (born.month, born.day))
        moron_df['age'] = moron_df['age'].apply(age)
        logging.info('Get age by the date of birth')
        # moron_df.to_excel(f'{ruta_normalized}/data1.xlsx')
        logging.info('End of the process for university of Moron')

        try:
            # Create .TXT file in files folder
            moron_df.to_csv(
                f'{ruta_files}/ET_Universidad_de_Moron.txt', index=None, sep='\t')
            logging.info('File .txt created in files folder')
            logging.info('Finish')
        except:
            logging.error('Error to creating txt file')

except:
    logging.error('General error at data normalization')


