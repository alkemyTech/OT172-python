from doctest import ELLIPSIS_MARKER
import xml.etree.ElementTree as ET
from functools import reduce
import re
from typing import Counter
import pathlib
import os
import sys
import operator
import pandas as pd

path_p = (pathlib.Path(__file__).parent.absolute())

sys.path.append(f'/{path_p}/lib')
from mapReduce import *
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
    for key, value in data2.items():
        if key in data1.keys():
            data1.update({key: data1[key]+value})
        else:
            data1.update({key: value})
    return data1


def order_scores(data):
    """
    Order scores sort the input dict by their values
    """
    return sorted((value, key) for (key,value) in data.items())


def main():
    tree = ET.parse(
        f'{path_p}dataset/112010 Meta Stack Overflow/posts.xml')
    root = tree.getroot()
    chunked_data = chunkify(root, 50)
    tags = list(map(mapper_1, chunked_data))
    filt_NAns_tags = list(filter(None, tags))
    tags_mapped = dict(reduce(reduce_counters, filt_NAns_tags))
    result=list(order_scores(tags_mapped))
    df=pd.DataFrame(result[0:11], columns={'tag', 'number'})
    print(df)

if __name__ == '__main__':
    main()