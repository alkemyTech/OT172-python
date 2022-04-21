
from doctest import ELLIPSIS_MARKER
from genericpath import exists
from importlib.resources import path
import logging
from statistics import mean
from time import time
from tkinter import font
import xml.etree.ElementTree as ET
from functools import reduce
import re
from typing import Counter
import pathlib
from os import path
import sys
import operator
import pandas as pd
import pathlib
import logging
import logging.config
from collections.abc import Iterable
import time

from sympy import root
from rich.tree import Tree
from rich import print as rprint
from art import *


  
sys.path.append(f'/{path_p}/lib')
#from mapReduce import *

#######################################################################################################
# Puntaje promedio de las repuestas con mas favoritos
############################################################################################################

# Abordaje del problema

# 1- A partir de los datos, se obtienen la cantidad de favoritos por respuesta y su score
# 2- El resultado del paso 1, se usa como input en una función filter, que se encarga de eparar aquellas entradas
# sin resultado alguno ("None")
# 3- Un primer reductor, reduce los datos del chunck procesado.
# 4- Mediante una nueva secuencia de filtro y reduce, se obtiene el formato requerido. y finalmente
# se selecciona la cantidad pedida por la tarea

from time import sleep
from rich.console import Console

console = Console()
tasks = [f"task {n}" for n in range(1, 11)]

def task(msj):
    with console.status("[bold green]Working hard...") as status:
        console.log(f"{msj} complete")
        logger.info(msj)

def get_tree():

    tree = Tree("[blue]🗃️ OT172-python")
    tree.add("[blue]📁 airflow")
    tree.add("📁 [green]bigdata").add("[green]📁 dataset").add('[green]📁 Stack Overflow 11-2010').add('[green]📁 112010 Meta Stack Overflow').add("📄[red]posts")
    tree.add("[blue]📁 dataGroupA")
    tree.add("[blue]📁 data_group_E")
    tree.add("[blue]📁 files")
    tree.add("[blue]📁 logger")
    tree.add("[blue]📁 map_reduce")
    rprint('\nDirectorio\n')
    rprint(tree)


"""Function to configure the code logs
Args: relativ path to .log file"""
log_file_path = (f'{pathlib.Path(__file__).parent }/logging/logging_GB/logging.cfg')
print(log_file_path)
logging.config.fileConfig(log_file_path)
 
logger = logging.getLogger('logger')

def chunk_data(iterable_data, len_of_chunk):
    """ 
    Se divide la data en partes para poder trabajarla
    arg: iterable: lista de datos obtenida
         len_of_chunk: cantidad de partes en las que se dividira la lista
    retunr: Retorna la lista dividida en partes
    """
    try:
        for i in range(0, len(iterable_data), len_of_chunk):
            yield iterable_data[i:i +len_of_chunk]

    except:
        if len_of_chunk < 0:
            raise logger.error(f'El valor de el argumento len_of_chunk debe ser mayor a 0 {ValueError}')
        elif not isinstance(len_of_chunk, int):
            raise logger.error(f'Debe ingresar un número entero {TypeError}')



def get_fav_scores(data):
    """
Args:

StackOverflow post xml file

Output:  Dictionary wiyh "count of favorites: score of the post"

The function obtains, from the input, a dictionary with 
the number of favorites of a post as the key, and the 
score of the post as the value.
The try/exception block contains a relation in which, 
if the score is 0, the exception acts, returning "None".
This is to avoid future inconvenience when dividing by 
0, since the ultimate goal is to obtain a ratio
    """
    post= data.attrib['Id']
    None_post=[]
    try:
        score = (data.attrib['Score'])
        favorites = data.attrib['FavoriteCount']
        data= {favorites: int(score)}
    except:
        data= {f'NULL Post Id': post}
    return data
     



def reduce_fav_scores(data1, data2):
    """Args:

    input:    
    data1, data2:  Outputs from get_fav_scores function

    output:
    a dictionary with unique data 1 and data 2 keys, and
    score values

    This function acts together with the "reduce" method. 
    It will act sequentially. taking two data as input, 
    returning a single data, which is taken again as input 
    together with the next data.
    The goal is to take the dictionary that comes from the  
    "get_fav_scores" function, as individual dictionaries, 
    and get a single dictionary with many keys.
    The loop acts by updating the values of each key if it 
    is present in both input data, or adding it to the output 
    dictionary along with its value, if it is not already in 
    the output dictionary.
    """
    try:
        for key, value in data2.items():
            if key== 'NULL Post Id':
                data1=data1
            else:
                try:
                    if key in data1.keys():
                        data1.update({key: round((((data1[(key)]))+(value))/2, 2)})
                    else:
                        data1.update({key: value})
                except:
                    if not isinstance(data1[key], int) or not isinstance(value, int):
                        logger.error(f'Los valores en los diccionarios deben ser del tipo "int" {TypeError}')
            if 'NULL Post Id' in data1.keys():
                del data1['NULL Post Id']
            return (data1)
    except TypeError as e:
        if not isinstance(data1, dict):
           raise logger.error(f'Debe ingresar un diccionario en el argumento data1 {e}')
        if not isinstance(data1, dict):
           raise logger.error(f'Debe ingresar un diccionario en el argumento data2 {e}')


def mapper_prom_score(chunck):
    """
    Args:

    A chunck from "chunckify" function

    output: Dict

    mapper that applies the get_fav_scores and reduce_fav_scores 
    functions, along with the map method.
    This method takes an iterable object i and applies the designated 
    function to the different iterations. Between the two mentioned 
    functions, there is a filter function that will eliminate those
    inputs that are "None"
    """
    None_post_list=[]
    for i in range(len(chunck)):
            dict_fav_scores = list(map(get_fav_scores, chunck))
            try:
                fav_scores_count = (reduce(reduce_fav_scores, dict_fav_scores))
                return fav_scores_count
            except:
                None_post_list.append(i)
                return
       
def main(path):
        file=path.split('/')[-1]
        start_time = time.time()
        rprint('[green]------------------------------------------------------------------------------')
        rprint('[green]------------------------------------------------------------------------------')
        logger.info('Iniciando ejecución')
        rprint(f'Cargando datos desde archivo {file}')
        print('')
        tree = ET.parse(path)
        root = tree.getroot()
        print(type(root))
        get_tree()
        print('')
        tree = ET.parse(path)
        root = tree.getroot()
        print(type(root))
        task('Dividiendo el dataset...')
        rprint('[green]-----------------------------------------------------------------------------')
        print('Iniciando proceso MapReduce')
        print('')
        chunked_data = chunk_data(root, 50)
        task('MapReduce etapa 1: mapper...')
        tags = list(map(mapper_prom_score, chunked_data))
        filter_data= list(filter(None,tags))
        task('MapReduce etapa 2: reducer...')
        tags_red = (reduce(reduce_fav_scores, filter_data))
        task('MapReduce  finalizado')
        rprint('[green]-----------------------------------------------------------------------------')
        task('Obteniendo resultados...')
        values = list(tags_red.values())
        mean_score = mean(sorted(values)[-10:])
        print('')
        task(f'[blue]El puntaje promedio de los tags con menos respuestas aceptadas fue [green]{mean_score}')
        print('')
        task(f'[blue]Tiempo de ejecución {time.time() - start_time}')
        print('')
        rprint('[green]-----------------------------------------------------------------------------')
        rprint('[green]-----------------------------------------------------------------------------')
        tprint('GRACIAS!!!', font='#')
        rprint('[green]-----------------------------------------------------------------------------')
        rprint('[green]-----------------------------------------------------------------------------')



if __name__ == '__main__':
    main('C:/Users/Jsolchaga86/Desktop/Proyectos/Alkemy/OT172-python/bigdata/dataset/Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml')


