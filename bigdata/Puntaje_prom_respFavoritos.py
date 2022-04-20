
from doctest import ELLIPSIS_MARKER
from importlib.resources import path
from statistics import mean
import xml.etree.ElementTree as ET
from functools import reduce
import re
from typing import Counter
import pathlib
import os
import sys
import operator
import pandas as pd
import pathlib

path_p = (pathlib.Path(__file__).parent.absolute())
  
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
    try:
        score = int(data.attrib['Score'])
        filt = 1/score
        favorites = data.attrib['FavoriteCount']
        return {favorites: score}
    except:
        return


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
    for key, value in data2.items():
        if key in data1.keys():
            data1.update({key: round(((data1[(key)])+value)/2, 2)})
        else:
            data1.update({key: value})
    return (data1)


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

    dict_fav_scores = list(map(get_fav_scores, chunck))
    filt_data = list(filter(None, dict_fav_scores))
    try:
        fav_scores_count = (reduce(reduce_fav_scores, filt_data))
    except:
        return
    return fav_scores_count


def top_10_fav_scores(data):
    """Sorts the input dictionary data, according to the value of 
    its keys, takes the keys and values according to that order 
    and returns a list, with two list lists inside, one with the 
    top 10 keys and another with the corresponding values
    """
    sort_data = dict(sorted(data.items(), key=lambda x:int(x[0])))
    tot_keys= list(sort_data.keys())
    top_10_keys= tot_keys[-10:]
    tot_scores= list(sort_data.values())
    return [tot_scores[-10:], top_10_keys]


def main():
    tree = ET.parse(
        f'{path_p}dataset/112010 Meta Stack Overflow/posts.xml')
    root = tree.getroot()
    chunked_data = chunkify(root, 50)
    tags = list(map(mapper_prom_score, chunked_data))
    filt_data = list(filter(None, tags))
    tags_red = (reduce(reduce_fav_scores, filt_data))
    result = top_10_fav_scores(tags_red)
    mean_score = mean(result[0])
    print(f' \nEl puntaje promedio de los 10 post con mas favoritos fue {mean_score}\nLos 10 post con mas favoritos tuvieron las siguientes cantidades; {result[1]}')


if __name__ == '__main__':
    main()
