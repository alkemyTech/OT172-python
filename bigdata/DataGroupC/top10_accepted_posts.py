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

tag_regex = re.compile("<([^>]+)>")
parent_folder = Path(__file__).resolve().parent.parent.parent
filename = os.path.join(parent_folder,'datasets/posts.xml')

def tag_extractor(row):
    post_type = row.attrib.get('PostTypeId')
    if post_type != "1":
        return
    # Extraer atributo tag
    tags = row.attrib.get('Tags')
    if tags:
        accepted_id = row.attrib.get('AcceptedAnswerId')
        tag_list = tag_regex.findall(tags)
        # Acumular solo si la respuesta es aceptada
        if accepted_id:
            return tag_list
    return

def tag_reducer(tag1, tag2):
    c = Counter(tag1)
    c.update(tag2)
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
    return reduce(tag_reducer, mapa)

print(xml_map_reduce())
