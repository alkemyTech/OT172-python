"""
Procesar y ejecutar datos para obtener los siguientes requerimientos:
Relación entre cantidad de palabras en un post y su cantidad de respuestas

ticket: https://alkemy-labs.atlassian.net/browse/PT172-134
"""
from collections import Counter
from functools import reduce
from html.parser import HTMLParser
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

parent_folder = Path(__file__).resolve().parent.parent.parent
filename = os.path.join(parent_folder,'datasets/posts.xml')

class BodyHTMLParser(HTMLParser):
    """ Extrae numero de palabras del html de body - posts """
    def __init__(self):
        super().__init__()
        self.reset()
        self.container = []

    def handle_data(self, data):
        val = data.lower().strip()
        if val:
            seq = val.split()
            self.container.extend(seq)

    def get_list(self):
        return self.container

    def get_word_count(self):
        return len(self.container)

def word_answer_extractor(row):
    post_type = row.attrib.get('PostTypeId')
    if post_type != "1":
        return
    # Obtener contenido de body
    body = row.attrib.get("Body")
    if not body:
        return
    else:
        parser = BodyHTMLParser()
        parser.feed(body)
        word_count = parser.get_word_count()

    # Obtener conteo de respuestas
    ans_counts = row.attrib.get("AnswerCount")
    if ans_counts and word_count:
        return word_count, int(ans_counts)
    return

def word_answer_reducer(c, tupla):
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

def get_row_list(num):
    gen = xml_generator()
    lista = []
    for _ in range(num):
        next_elem = next(gen)
        lista.append(next_elem)
    return lista

def xml_map_reduce():
    vals = get_row_list(8)
    mapa = map(word_answer_extractor, vals)
    mapa = list(filter(lambda x: x, mapa))
    contador = Counter()
    return reduce(word_answer_reducer, mapa, contador)
