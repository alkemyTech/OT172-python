"""
Procesar y ejecutar datos para obtener los siguientes requerimientos:
Top 10 tipo de post con mayor respuestas aceptadas

ticket: https://alkemy-labs.atlassian.net/browse/PT172-134
"""
from collections import Counter
from functools import reduce
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

parent_folder = Path(__file__).resolve().parent.parent.parent
filename = os.path.join(parent_folder,'datasets/posts.xml')

def tag_extractor(row):
    post_type = row.attrib.get('PostTypeId')
    if post_type != "1":
        return
    owner_id = row.attrib.get('OwnerUserId')
    fav_counts = row.attrib.get('FavoriteCount')
    if fav_counts and owner_id:
        return owner_id, int(fav_counts)
    return

def owner_favorites_reducer(c, tupla):
    c.update({tupla[0]: tupla[1]})
    return c

def xml_generator():
    """ Retorna los rows del xml """
    with open(filename, 'r') as handle:
        parser = ET.XMLPullParser()
        while True:
            new_line = handle.readline()
            if not new_line:
                break
            parser.feed(new_line)
            for item in parser.read_events():
                elem = item[1]
                if elem.tag == 'row':
                    yield elem

def get_tag_list(num):
    gen = xml_generator()
    lista = []
    for _ in range(num):
        next_elem = next(gen)
        lista.append(next_elem)
    return lista

def xml_map_reduce():
    vals = get_tag_list(8)
    mapa = list(map(tag_extractor, vals))
    mapa = list(filter(lambda x: x, mapa))
    contador = Counter()
    return reduce(owner_favorites_reducer, mapa, contador)
