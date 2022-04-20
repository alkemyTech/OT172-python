
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

path_p = (pathlib.Path(__file__).parent.absolute()).parent
  
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

def get_body_views(data):
        """
        get body score obtained, from the source, a dictionary that
        contains as key the number of words of the post text, and 
        body/viewsCount as value.
        the text of the message contains a regular expression, which 
        with the re module filters the text corresponding to HTML code
        """
        try:
            data=data[0]
            body= data.attrib['Body']
            body= re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=(?!\S))', body)
            counter_words= str(len(body))
            views= int(data.attrib['ViewCount'])
            return {counter_words: round(int(counter_words)/views,2)}
        except:
            return
   

    


def reduce_scores(data1, data2):
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
    The objective is to take the dictionary that comes from the  
    "get_body_views" function, as individual dictionaries, 
    and get a single dictionary with many keys and the mean 
    between values.
    The loop acts by updating the values of each key if it 
    is present in both input data, or adding it to the output 
    dictionary along with its value, if it is not already in 
    the output dictionary.
    """
    try:
        for key, value in data2.items():
                try:
                    if key in data1.keys():
                        data1.update({key: round((((data1[(key)]))+(value))/2, 2)})
                    else:
                        data1.update({key: value})
                except:
                    if not isinstance(data1[key], int) or not isinstance(value, int):
                        logger.error(f'Los valores en los diccionarios deben ser del tipo "int" {TypeError}')
        return (data1)
    except TypeError as e:
        if not isinstance(data1, dict):
           raise logger.error(f'Debe ingresar un diccionario en el argumento data1 {e}')
        if not isinstance(data1, dict):
           raise logger.error(f'Debe ingresar un diccionario en el argumento data2 {e}')


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
        get_tree()
        print('')
        task('Dividiendo el dataset...')
        rprint('[green]-----------------------------------------------------------------------------')
        print('Iniciando proceso MapReduce')
        print('')
        chunked_data = chunk_data(root, 50)
        task('MapReduce etapa 1: mapper...')
        maped=list(map(get_body_views, chunked_data))
        filtered=list(filter(None, maped))
        task('MapReduce etapa 2: reducer...')
        red= reduce(reduce_scores, filtered)
        task('MapReduce  finalizado')
        rprint('[green]-----------------------------------------------------------------------------')
        task('Obteniendo resultados...')
        df = pd.DataFrame(list(red.items()),columns = ['n_words','n_words/views'])
        result= df[-10:] 
        print(result)
        print('')
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
