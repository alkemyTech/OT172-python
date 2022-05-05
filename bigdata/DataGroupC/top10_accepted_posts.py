"""
Procesar y ejecutar datos para obtener los siguientes requerimientos:
Top 10 tipo de post con mayor respuestas aceptadas

ticket: https://alkemy-labs.atlassian.net/browse/PT172-134
"""
from collections import Counter
from functools import reduce
import multiprocessing as mp
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

parent_folder = Path(__file__).resolve().parent.parent.parent
filename = os.path.join(parent_folder,'datasets/posts.xml')
cpu_count = 3
tag_regex = re.compile("<([^>]+)>")

def tag_extractor(row):
    """ Extrae los tags de cada fila xml """
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

def processfile(filename, start=0, end=0):
    """ Retorna los rows del xml """
    counter = Counter()
    result = []
    with open(filename, 'r') as handle:
        handle.seek(start)
        lines = handle.readlines(end - start)
        parser = ET.XMLPullParser()
        for line in lines:
            if not line:
                break
            parser.feed(line)
            for item in parser.read_events():
                elem = item[1]
                if elem.tag == 'row':
                    tags = tag_extractor(elem)
                    if tags:
                        counter.update(tags)
    if counter:
        result.append(counter)
    return result

def counter_reducer(a, b):
    a.update(b)
    return a

if __name__ == "__main__":
    # Tomamos chunks de 256 KB
    filesize = os.path.getsize(filename)
    split_size = 1024 * 256
    pool = mp.Pool(cpu_count)
    cursor = 0
    results = []
    with open(filename, 'r') as fh:
        for chunk in range(filesize // split_size + 1):
            if cursor + split_size > filesize:
                end = filesize
            else:
                end = cursor + split_size
            fh.seek(end)
            fh.readlines()
            end = fh.tell()
            proc = pool.apply_async(processfile, args=[filename, cursor, end])
            results.append(proc)
            cursor = end
    pool.close()
    pool.join()
    processfile_result = [proc.get() for proc in results]
    processfile_result = filter(lambda x: x, processfile_result)
    reduced = reduce(counter_reducer, processfile_result)
    print(reduced[0].most_common(10))
