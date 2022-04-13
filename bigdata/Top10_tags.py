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
from mapReduce import *
################################################################################################################

# Criterios de aceptación:
# Procesar y ejecutar datos para obtener los siguientes requerimientos:

# Top 10 tags sin respuestas aceptadas
# Relación entre cantidad de palabras en un post y su cantidad de visitas
# Puntaje promedio de las repuestas con mas favoritos

# ##############################################################################################################
# # #Top 10 tags sin respuestas aceptadas
################################################################################################################

# Defino la funcion chunkify


def chunkify(data, len_of_chunks):
    for i in range(0, len(data), len_of_chunks):
        yield data[i:i + len_of_chunks]

# Defino la funcion para obteer los tags


def get_tags_NA_ans(data):
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

def calculate_top_10(data):
    return data[0], data[1].most_common(10)
    

def split_tag_words(data):
    dic={}
    for i in data:
        for key, value in i.items():
            dic.update({key:value})
    return dic


def mapper_1(chunck):
    NAns_tags = list(map(get_tags_NA_ans, chunck))
    filt_tags = list(filter(None, NAns_tags))
    dict_ans = list(map(split_tag_words, filt_tags))
    try:
        tags_count = (reduce(reduce_counters, dict_ans))
    except:
        return
    return tags_count



def reduce_counters(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1.update({key: data1[key]+value})
        else:
            data1.update({key: value})
    return data1


def top_10_fav_scores(data):
    return sorted((value, key) for (key,value) in data.items())


def main():
    tree = ET.parse(
        '/home/juan/Alkemy/OT172-python/bigdata/Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml')
    root = tree.getroot()
    chunked_data = chunkify(root, 50)
    tags = list(map(mapper_1, chunked_data))
    filt_NAns_tags = list(filter(None, tags))
    tags_mapped = dict(reduce(reduce_counters, filt_NAns_tags))
    result=list(top_10_fav_scores(tags_mapped))
    df=pd.DataFrame(result[0:11], columns={'tag', 'number'})
    print(df)

if __name__ == '__main__':
    main()