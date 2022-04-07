import json
import os
import shutil
import fileinput
import pathlib
from datetime import date
from pendulum import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from decouple import config
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import urllib.request
import urllib.parse
import difflib
import psycopg2
from sympy import Q

# Credentials

PG_USERNAME= config('PG_USERNAME', default='')
PG_PASSWORD= config('PG_PASSWORD', default='')
PG_SCHEMA= config('PG_SCHEMA', default='')
PG_HOST= config('PG_HOST', default='')
PG_CONNTYPE=config(' PG_CONNTYPE', default='')
PG_ID= config('PG_ID', default='')
PG_PORT= config('PORT', default='')

def generate_sql(df, id, univ):
    """The function creates the files with the sql queries for 
    the required universities
    Args: 
    
    df:   A df with the initial names of the columns corresponding 
    to the target university

    univ: The university from which you want to obtain the query

    1- The function takes the names of the columns of the df of the 
    university, and passes them to lowercase
    2- Designate a dictionary with the desired name for each column as 
    key, and the list of possible column names, among which is the name 
    of the input university as values.
    For each column name of the input university, it builds a dictionary
    with the name that the column will have as key, and the name 
    provided as value

    Next, the location of the .sql file that will be used as a template 
    is indicated, and the values that will replace the values of the 
    template (which come from the previously defined dictionary) are 
    indicated. These names correspond to the previous names that the 
    table has, since the final names of the columns are always the same.

    Finally, create a copy of the template, iterate over the lines of the 
    file and replace the corresponding values to customize the file


    """
    path = (pathlib.Path(__file__).parent.absolute()).parent
    column_list= df.columns
    column_list = [x.lower() for x in column_list]
    key_dict = {'university': ['universidad', 'university', 'universities','univiersities', 'universidades'],
                    'career': ['carrera', 'careers', 'carrerra', 'careras', 'career', 'carreras'],
                    'nacimiento': ['fecha_nacimiento', 'birth_date', 'birth_dates', 'nacimiento', 'fechas_nacimiento'],
                    'inscription_date': ['fechaiscripccion','inscription_dates','inscription_date','fecha_de_inscripcion', 'fechas_de_inscripcion','inscription_date', 'inscription_date', 'fechainscripcion'],
                    'location': ['location', 'locaations', 'localidad', 'locations'], 
                    'postal_code':['codigos_postales', 'codigo_postal', 'codgoposstal', 'postal_code'],
                    'gender': ['sexo'],
                    'name': ['name', 'names', 'nombre', 'nombrre', 'nombres'],
                    'email': ['email', 'emails', 'eemail','correos_electronicos' ,'correo_electronicos', 'correo_electronico']}
    query_columns={}
    values_l=[]
    keys_l=[]
    for i in column_list:
        for d in key_dict:
            if i in key_dict[d]:
                values_l.append(i)
                keys_l.append(d)
    for key, value in zip(keys_l, values_l):
        query_columns[key] = value

    query_template_filename = f'{path}/dags_dinamicos/query_template.sql'
    nam= query_columns['name']
    gen= query_columns['gender']
    email= query_columns['email']
    nac= query_columns['nacimiento']
    in_dat= query_columns['inscription_date']
    univ= query_columns['university']
    car= query_columns['career']
    try:
        zone= query_columns['location']
    except:
        pc_zone= query_columns['postal_code']

    new_filename = f'{path}/include/'+id+'.sql'
    shutil.copyfile(query_template_filename, new_filename)

    for line in fileinput.input(new_filename, inplace=True, backup=f'.bak'):
                    line= line.replace("idimput", id)
                    line= line.replace("selectuniv", univ)
                    line= line.replace("universityimput", univ)
                    line= line.replace('nameinput',nam)
                    line= line.replace('genderinput',gen)
                    line= line.replace('emailinput',email)
                    line= line.replace('nacimientoinput',nac)
                    line= line.replace('inscription_dateinput',in_dat)
                    line= line.replace('careerinput',car)
                    try:
                        line= line.replace('locationinput',zone)
                        line= line.replace('postalcodeinput as postal_code,', ' ')
                    except:
                        line= line.replace('postalcodeinput',pc_zone)
                    line= line.replace('locationinput as location,', '                               ')
                    print(line, end="")
    os.remove(f'{new_filename}.bak') 




def main(id_univ_list):
    """
    The main function generates an sql query to all the columns of the 
    input table, and returns a df of a row of that query. The objective is 
    to have a quick query with the names of the columns, to use as input 
    to the create queries function.

    Args: 

    id_univ_list:   a list with the name of the table as the first value, 
    and the name of the target university as the second value

    df of a row of the table that contains the target university

    The function makes the following query
    'SELECT * FROM {id_} fetch first 1 rows only'

    that will return a table with all the columns and a single row, from
    the given table.
    The query is made through the pandas read_sql function, connecting
    through psycopg2 to the database, with the assigned credentials
    
    """
    for dict in id_univ_list:
        id_= dict[0]
        univ_= dict[1]
        
        query= f'SELECT * FROM {id_} fetch first 1 rows only'

          
        params= ({'host': 'training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com',
        'port': 5432,
         'database': 'training',
         'user':'alkymer',
         'password':'alkymer123'})

        conn=psycopg2.connect(**params)
        df=pd.read_sql(query, conn)
        generate_sql(df, id=id_,univ= univ_)
        print(id_+' generated')




