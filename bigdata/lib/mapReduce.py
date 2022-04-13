from doctest import ELLIPSIS_MARKER
import xml.etree.ElementTree as ET
from functools import reduce
import re
from typing import Counter

# Defino la funcion chunkify
def chunkify(data, len_of_chunks):
    for i in range (0, len(data), len_of_chunks):
        yield data[i:i + len_of_chunks]

# Defino la funcion para obteer los tags
def get_tags(data):
    try:
        tags= data.attrib['Tags']
    except:
        return
    tags= re.findall('<(.+?)>', tags)
    body= data.attrib['Body']
    body= re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=(?!\S))', body)
    counter_words= Counter(body)
    return tags, counter_words

def split_tag_words(data):
    return dict([[tag, data[1].copy()] for tag in data[0]])


def reduce_counters(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key:value})
    return data1  

def calculate_top_10(data):
    return data[0], data[1].most_common(10)
    

