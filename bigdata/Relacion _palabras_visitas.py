
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
#################################################################################################################
#Relación entre cantidad de palabras en un post y su cantidad de visitas
#################################################################################################################

import pandas as pd

path_p = (pathlib.Path(__file__).parent.absolute())

sys.path.append(f'/{path_p}/lib')
from mapReduce import *

def chunkify(data, len_of_chunks):
    for i in range(0, len(data), len_of_chunks):
        yield data[i:i + len_of_chunks]



def get_body_scores(data):
    try:
        body= data.attrib['Body']
        score= int(data.attrib['Score'])
        filt=1/score
    except:
        return
    body= re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=(?!\S))', body)
    counter_words= str(len(body))
    views= int(data.attrib['ViewCount'])
    if views !=0:
        return {counter_words: round(int(counter_words)/score,2)}
    else:
        pass
    


def reduce_scores(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[(key)]=round(((data1[(key)])+value)/2,2)
        else:
            data1.update({key:value})
    return (data1)  

def mapper_2(chunk, n):
    tags=list(map(get_body_scores, chunk))
    return tags


def main_2():
    tree = ET.parse(
        f'{path_p}dataset/112010 Meta Stack Overflow/posts.xml')
    root= tree.getroot()
    tags=chunkify(root, 50)
    tags= list(mapper_2(root,0))
    tags=list(filter(None, tags))
    red= reduce(reduce_scores, tags)
    df = pd.DataFrame(list(red.items()),columns = ['n_words','n_words/views']) 
    print(df)
    return df


if __name__ == '__main__':
    main_2()
