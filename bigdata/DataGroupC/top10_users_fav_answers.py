"""
Procesar y ejecutar datos para obtener los siguientes requerimientos:
Top 10 tipo de usuarios con mayor respuestas favoritas

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

def tag_extractor(row):
    post_type = row.attrib.get('PostTypeId')
    if post_type != "1":
        return
    owner_id = row.attrib.get('OwnerUserId')
    fav_counts = row.attrib.get('FavoriteCount')
    if fav_counts and owner_id:
        return owner_id, int(fav_counts) # retorna una tupla
    return

def processfile(filename, start, end):
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
                        counter.update({tags[0]: tags[1]})
    if counter:
        result.append(counter)
    return result

def owner_favorites_reducer(c1, c2):
    c1.update(c2)
    return c1

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
    reduced = reduce(owner_favorites_reducer, processfile_result)
    print(reduced[0].most_common(10))