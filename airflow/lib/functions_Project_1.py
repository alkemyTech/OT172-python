from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sympy import Id
import pandas as pd
import logging
import os
import sys
from decouple import config
import pathlib

path_p = (pathlib.Path(__file__).parent.absolute()).parent
  

def logger(relative_path):

    """Function to configure the code logs

    Args: relativ path to .log file"""
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                        filename=f'{path_p}/{relative_path}', encoding='utf-8', level=logging.ERROR)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None

# Data extraction function through database queries
# Start connecting to the database through the Postgres operator`s Hook
def extraction(database_id:str, table_id:str):

    """Args: 

    datbase_id: id from the pg-database 
                (previously created 
                from the airflow interface
    table_id:  id of the table to consult

    output: pandas dataframe with queries result
               a csv file with the name of the required table
               is saved locally in the "files" folder 
    

    Extraction function through queries to the database.
    Through the PostgresHook hook (https://airflow.apache.org/,\ndocs/apache-airflow-providers-postgres/stable/\n    _api/airflow/providers/postgres/hooks/postgres/index.html) 
    the connection to a posgreesql database previously created
    in the Airflow interface,taking as a parameter the ID designated 
    to the connection
    """

    from sqlalchemy import text
    logging.info('Connecting to db')
    if (not isinstance(database_id, str) or not isinstance(table_id, str)):
            logging.ERROR('input not supported. Please enter string like values')

    elif ((isinstance(database_id, str) or  isinstance(table_id, str))):
        hook = PostgresHook(postgres_conn_id=database_id)
        conn = hook.get_conn()
        logging.info(f'Conected to {table_id}')

    else:
        logging.error('ERROR: database connection failed')


# SQL query: To execute the query with the Hook, it must be passed as a string to the function
# pd.read_sql, along with the conn object that establishes the connection.
# The .sql file is opened and the text is saved in the query variable
    logging.info('opening sql file')
    if os.path.exists(f'{path_p}/include/{table_id}.sql'):
        try:        
            with open(str(f'{path_p}/include/{table_id}.sql')) as file:
                try:
                    query = str(text(file.read()))
                    logging.info(f'Extracting data to {file}')
                except:
                    logging.ERROR('cant read sql file')
        except:
            logging.ERROR('Open .sql file failed')
    else:
            logging.error(f'file not exist')

# The output of this function is a df with the selected rows and columns
# Finally, the df is saved as .csv
    try:
        logging.info('executing query')
        df = pd.read_sql(query, conn)
        logging.info('query successfull')
    except:
        logging.ERROR('query fail')
    df.to_csv(f'{path_p}/files/{table_id}.csv', sep='\t')
    logging.info('Data saved as csv')
    return df


  
def load_s3(id_conn,  univ):
    bucket_name=get_conn_param( id_conn, 'schemma')
    key=get_conn_param( id_conn, 'extra')

    hook = S3Hook(id_conn)
    hook.load_file(filename=f'{path_p}/files/{univ}.txt',
                   key=key, bucket_name=str(bucket_name))

# Data transformation functions


# normalize characters
# removes spaces at the beginning or end of strings, hyphens and
# convert words to lower case



def normalize_characters(column):
    """
    Args: a df column

    output: a df column without special characters

    The function takes the string values of the 
    column of a df and normalizes the special characters
    """
 
    try:
        column = column.apply(lambda x: str(
            x).replace(' \W'+'*'+'\W', '\W'+'*'+'\W'))
        column = column.apply(lambda x: str(
            x).replace('\W'+'*'+'\W ', '\W'+'*'+'\W'))
        column = column.apply(lambda x: str(x).replace('-', ' '))
        column = column.apply(lambda x: str(x).replace('_', ' '))
        column = column.apply(lambda x: x.lower())
    except:
        logging.ERROR('normalizing failed')
    return column

# transform_df
# transform the data in a pd.DataFrame format

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
# query. The resulting df was passed to the dictionary, establishing the postl code variable as key (also defined in the
# sql query table. Finally, the values ​​of the zip codes in the query table are called, in the dictionary
# previously defined, resulting in the corresponding localities column).
# line 17: only the required columns were selected


def transform_df(df, university_id:str):
    """
    
    This function si adapted to transform the data
    of all the universities in the database
         
    Args:
        df: a p.ataFram from sql qeries
        university_id: id from the University

    output: a df and a locally saved file ('.txt)

    The function transforms the data as follows:

    Normalizes the content of string type columns 
    with special characters through the 
    'normalize_characters' function

    gender: transform strings starting with m to 'male, and the 
    rest to 'female' or 'not identified as male or female gender'

    name: having treated the special characters of this column with 
    the "normalizing data" function, they are separated into 
    'first_name' and last_ame'. If this is not possible, everything 
    is assigned to 'last_name'

    inscription_date: sets '/' for separation and '%Y/%m/%d'' as date
    format

    age: Three possible cases are taken into account for the birth_date
            formats '%d/%m/%Y' and '%Y/%m/%d' : the age is obtained by 
                                                subtracting the date of 
                                                birth from the current year
            'dd/Month/YY' format: by which the last two positions are taken
            from the string (corresponding to the ten of the year), it is 
            converted to 'int' and together with the current year 'curr', the
            age is obtained by the following formula: 100+(curr- year of birth)

    location and postal_code: depending on the column in the table, the 
    values ​​are used as input to define a dictionary, by which the values ​​of the 
    missing column will be called"""

  
    logging.info(f'normalizing data')

    #strngs with special characters
    df['university'] = normalize_characters(df['university'])
    df['career'] = normalize_characters(df['career'])
    df['name'] = normalize_characters(df['name'])
    df['gender'] = normalize_characters(df['gender'])
    df['nacimiento']= df['nacimiento'].apply(lambda x: str(x).replace('-', '/'))
    #gender
    try:
        df['gender'] = df['gender'].apply(
            lambda x: 'male' if x[0] == 'm' else 'female')
    except:
        logging.info('not identified as male or female gender')  

    #names
    try:
        df['first_name'] = df['name'].apply(lambda x: str(x).split(' ')[0])
        df['last_name'] = df['name'].apply(lambda x: str(x).split(' ')[1])
    except:
        df['first_name'] = 'NULL'
        df['last_name'] = df['name']
        
    #Dates
    try:
        old_date = pd.to_datetime(df['inscription_date'])
        df['inscription_date'].apply(lambda x: str(x).replace('-', '/'))
        df['inscription_date'] = pd.to_datetime(old_date, '%Y/%m/%d')
    except:
        logging.info("inscription enrollment date could not"+ 
                    "be transformed to '%Y/%m/%d' format."+
                    "current values will be kept in the column")

    #age    
    df['nacimiento']=df['nacimiento'].apply(lambda x: str(x).replace('-', '/'))
    if len(df['nacimiento'].apply(lambda x: str(x).split(' ')[0]))==4:
        df['nacimiento']= df['nacimiento'].apply(lambda x: x.strptime('Y%/%m/%d'))
        df['nacimiento']= df['nacimiento'].apply(lambda x: x.strftime('d%/%m/%Y'))

    curr= datetime.now()
    try:
        df['age'] = df['nacimiento'].apply(lambda x: (100+(int(str(curr.year)[2:4]) - int(x[7:9])) if len(x)== 9 else (curr.year - datetime.strptime(str(x), '%Y/%m/%d').year)))

    except:
        df['age'] = df['nacimiento'].apply(lambda x: curr.year - datetime.strptime(str(x), '%d/%m/%Y').year)

    #location and postal_code
    if 'postal_code' in df.columns:
        input= 'postal_code'
        output= 'location'
        key= 'codigo_postal'
        value= 'localidad'
    elif 'location' in df.columns:
        df['location'] = normalize_characters(df['location'])
        input= 'location'
        output= 'postal_code'
        key= 'localidad'
        value= 'codigo_postal'
        try:
            if os.path.exists(f'{path_p}/dataset/codigos_postales.csv'):
                df_postal_codes = (pd.read_csv(f'{path_p}/dataset/codigos_postales.csv'))
            else:
                logging.info('postal code file does not exist in the specified path')
            df_postal_codes['localidad'] = df_postal_codes['localidad'].apply(
                lambda x: x.lower())
            dict_postal_codes = dict(
            zip(df_postal_codes[key], df_postal_codes[value]))
            df[output] = df[input].apply(lambda x: dict_postal_codes[(x)])
        except:
            logging.info("values could not"+ 
                    "be transformed current"+
                    "values will be kept in the column")
        df['postal_code']= df['postal_code'].apply(lambda x: int(x))
        print(df.columns)
    # save_data
        df = df[['university', 'career', 'inscription_date', 'first_name',
                'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
        df.to_csv(f'{path_p}/files/{university_id}.txt', sep='\t')
    return None


def extraction_transform_data(database_id:str, table_id:str):
    """ction that is in charge of the complete process of 
    data extraction/transformation, through the 'extraction' 
    and 'transform_df' functions,
    see help(extraction)
        help(transform_df)"""
    df=extraction(database_id, table_id)
    df_t=transform_df(df, table_id)
    return df_t



def get_conn_param( conn_id, param):
        from airflow.hooks.base import BaseHook

        connection = BaseHook.get_connection(conn_id)
        if param== 'host':
            return connection.host
        elif param== 'schemma':
            return connection.schema
        if param== 'login':
            return connection.host
        elif param== 'extra':
            return connection.schema