from collections import Counter
from functools import reduce
import xml.etree.ElementTree as ET
import re


def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

# mal 
def get_tags_respuesta_true(xml):
    if ('Tags' not in xml.attrib or xml.attrib['Tags']
            == '') and 'AcceptedAnswerId' not in xml.attrib:
        return None
    else:
        return re.findall(r'<([^>]*?)>', xml.attrib['Tags'])


def mapper(data_chunk):
    mappedOne = list(map(get_tags_respuesta_true, data_chunk))
    mappedOne = list(filter(None, mappedOne))
    # aplanar lista
    mappedOne = [item for sublist in mappedOne for item in sublist]
    return dict(Counter(mappedOne))


def final_reducer(chunk1, chunk2):
    for key, value in chunk2.items():
        if key in chunk1.keys():
            chunk1[key] += value
        else:
            chunk1[key] = value
    return chunk1


if '__main__' == __name__:
    tree_xml = ET.parse(
        '/home/richarddiaz/Documentos/Stack_Overflow_11-2010/112010_Meta_Stack_Overflow/posts.xml')
    data_chunks = divide_chunks(tree_xml.getroot(), 100)

    mapped = list(map(mapper, data_chunks))
    mapped = reduce(final_reducer, mapped)
    mapped = sorted(mapped.items(), key=lambda x: x[1], reverse=True)[:10]
    mapped = [x[0] for x in mapped]

    print(mapped)
