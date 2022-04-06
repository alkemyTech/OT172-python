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

        query_template_filename = f'{path}/Dags_dinamicos/query_template.sql'
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


    

if __name__ == "__main__":
    main([['rio_cuarto_interamericana','-universidad-abierta-interamerican']])

