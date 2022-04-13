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

path_p = (pathlib.Path(__file__).parent.absolute()).parent

sys.path.append(f'/{path_p}/lib')
################################################################################################################

# Criterios de aceptación:
# Procesar y ejecutar datos para obtener los siguientes requerimientos:

# Top 10 tags sin respuestas aceptadas
# Relación entre cantidad de palabras en un post y su cantidad de visitas
# Puntaje promedio de las repuestas con mas favoritos

#################################################################################################################
#Relación entre cantidad de palabras en un post y su cantidad de visitas
#################################################################################################################

def chunkify(data, len_of_chunks):
    for i in range(0, len(data), len_of_chunks):
        yield data[i:i + len_of_chunks]


def get_body_scores(data):
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
    for key, value in data2[0].items():
        if key in data1.keys():
            data1[(key)]=round(((data1[(key)])+value)/2,2)
        else:
            data1.update({key:value})
    return (data1)  

def mapper_2(chunk):
    tags=list(map(get_body_scores, chunk))
    return tags


def main_2():
    tree= ET.parse('/home/juan/Alkemy/OT172-python/bigdata/Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml')
    root= tree.getroot()
    chunked_data=chunkify(root, 50)
    body_scores= list(map(mapper_2, chunked_data))
    body_scores_filt=list(filter(None, body_scores))
    result= list(reduce(reduce_scores, body_scores_filt))
    df = pd.DataFrame(list(result.items()),columns = ['n_words','views']) 
    print(df)
    return df

if __name__ == '__main__':
    main_2()

