################################################################################################################

# Criterios de aceptación:
# Procesar y ejecutar datos para obtener los siguientes requerimientos:

# Top 10 tags sin respuestas aceptadas
# Relación entre cantidad de palabras en un post y su cantidad de visitas
# Puntaje promedio de las repuestas con mas favoritos

# ##############################################################################################################
#  #Top 10 tags sin respuestas aceptadas
# ##############################################################################################################

# Abordaje del problema (Ejecutado por la función "mains"):
# El código está compuesto por un mapper que toma cada chunk y lo procesa de la siguiente forma:

# 1- A partir de los datos, se obtienen los tags y un conteo de la cantidad de veces que aparece cada uno. Dado que los tags
# aparecen en grupos, para poder ordenar aquellos que no tienen respuestas aceptadas, se definió como
# -1 el aporte de cada post sin respuestas aceptadas.
# 2- El resultado del paso 1, se usa como input en una función filter, que se encarga de eparar aquellas entradas
# sin resultado alguno ("None")
# 3- Un primer reductor, reduce los datos del chunck procesado.

# A continuación, se repite un nuevo "reduce", que reduce del set total de chunks, a uno solo. Este último
# pasa por una función de ordenado de los datos y finalmente, se obtiene un DataFrame con los
# requerimientos del ejercicio


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
from rich.console import Console
from sympy import root
from rich.tree import Tree
from rich import print as rprint
from art import *

path_p = (pathlib.Path(__file__).parent.absolute()).parent
  
sys.path.append(f'/{path_p}/lib')


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


def get_tags_NA_ans(data):
    """
    The function returns, from the input, a dictionary 
    with the post tags as keys, and the difference between
    accepted and unaccepted questions from the post. 
    For this, each accepted answer is counted as 1, and 
    each unaccepted answer is counted as -1.
    """
    try:
        tags = data.attrib['Tags']
    except:
        return
    if data.attrib['PostTypeId'] == '1':
        try:
            acc_ans = len(data.attrib['AcceptedAnswerId'])
            not_acc = 0
        except:
            not_acc = 1
            acc_ans = 0
        ans = acc_ans - not_acc
        tags = re.findall('<(.+?)>', tags)
    return [{tag: ans} for tag in tags]

def split_tag_words(data):
    """
    This function, using the "update" method,
    takes a list of dictionaries and converts
    them into a dictionary with multiple keys.
    """
    dic={}
    for i in data:
        for key, value in i.items():
            dic.update({key:value})
    return dic


def mapper_1(chunck):
    """
    The result of step 1 is used as input in a filter
    function, which is responsible for separating 
    those inputs without any result ("None")
    A first reducer reduces the data of the processed
    chunck.
    """
    NAns_tags = list(map(get_tags_NA_ans, chunck))
    filt_tags = list(filter(None, NAns_tags))
    dict_ans = list(map(split_tag_words, filt_tags))
    try:
        tags_count = (reduce(reduce_counters, dict_ans))
    except:
        return
    return tags_count



def reduce_counters(data1, data2):
    """
       This function acts together with the "reduce" method. 
    It will act sequentially. taking two data as input, 
    returning a single data, which is taken again as input 
    together with the next data.
    The objective is to take the dictionary that comes from the  
    "get_body_views" function, as individual dictionaries, 
    and get a single dictionary with many keys and the sum 
    of their values.
    The loop acts updating the values of each key if it 
    is present in both input data, or adding it to the output 
    dictionary  if it is not in 
    the output dictionary.
    """
    try:
        for key, value in data2.items():
                try:
                    if key in data1.keys():
                       data1.update({key: data1[key]+value})
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




def order_scores(data):
    """
    Order scores sort the input dict by their values
    """
    return sorted((value, key) for (key,value) in data.items())


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
        tags = list(map(mapper_1, chunked_data))
        filt_NAns_tags = list(filter(None, tags))
        task('MapReduce etapa 2: reducer...')
        tags_mapped = dict(reduce(reduce_counters, filt_NAns_tags))
        task('MapReduce  finalizado')
        rprint('[green]-----------------------------------------------------------------------------')
        task('Obteniendo resultados...')
        result=list(order_scores(tags_mapped))
        df=pd.DataFrame(result, columns={'tag', 'number'})
        print(df[-10:])
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

