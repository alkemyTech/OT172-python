
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
import pandas as pd

#################################################################################################################
#Relación entre cantidad de palabras en un post y su cantidad de visitas
#################################################################################################################

# Abordaje:
# En este caso, los requerimientos se resolvieron a partir de una función map, que aplica la función
# get_body_views, de la que se obtienen para cada chunck, diccionarios con el número de palabras del texto
# del post como clave, y la relación entre este valor y la cantidad de views como valor, devolviendo un diccionario
# Como en otros casos, se aplica un filtro y una operacion "reduce", y finalmente los datos se predentan en un DataFrame.



path_p = (pathlib.Path(__file__).parent.absolute())

sys.path.append(f'/{path_p}/lib')
from mapReduce import *

def get_body_views(data):
    """
    get body score obtained, from the source, a dictionary that
    contains as key the number of words of the post text, and 
    body/viewsCount as value.
    the text of the message contains a regular expression, which 
    with the re module filters the text corresponding to HTML code
    """
    try:
        body= data.attrib['Body']
    except:
        return
    body= re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=(?!\S))', body)
    counter_words= str(len(body))
    views= int(data.attrib['ViewCount'])
    if views !=0:
        return {counter_words: round(int(counter_words)/views,2)}
    else:
        pass
    


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
    for key, value in data2.items():
        if key in data1.keys():
            data1[(key)]=round(((data1[(key)])+value)/2,2)
        else:
            data1.update({key:value})
    return (data1)  


def main_2():
    tree = ET.parse(
        f'{path_p}/dataset/112010 Meta Stack Overflow/posts.xml')
    root= tree.getroot()
    tags=chunkify(root, 50)
    tags=list(map(get_body_views, tags))
    tags=list(filter(None, tags))
    red= reduce(reduce_scores, tags)
    df = pd.DataFrame(list(red.items()),columns = ['n_words','n_words/views']) 
    print(df)
    return df


if __name__ == '__main__':
    main_2()
