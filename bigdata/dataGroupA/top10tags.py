from decouple import config
from collections import Counter
from functools import reduce
import xml.etree.ElementTree as ET
import logging
import logging.config
import re
import pathlib
import sys


logging.config.fileConfig(f'{pathlib.Path(__file__).parent.absolute()}/logging.cfg')
logger = logging.getLogger('top_10_tags_A')

def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


""" Input: xml
    Output: listas con tags o None
    Solo si el post tiene tags y tiene el campo AcceptedAnswerId"""
def get_Tags_AcceptedAnswer(xml):
    if ('Tags' not in xml.attrib or xml.attrib['Tags']
            == '') and 'AcceptedAnswerId' not in xml.attrib:
        return None
    else:
        return re.findall(r'<([^>]*?)>', xml.attrib['Tags'])


""" Input: chunk de listas con xml's
    Output: diccionario con tags y cantidad de apariciones"""
def mapper(data_chunk):
    # obtener tags de cada post
    data_process = list(map(get_Tags_AcceptedAnswer, data_chunk))
    # eliminar Nones
    data_process = list(filter(None, data_process))
    # aplanar lista
    data_process = [item for sublist in data_process for item in sublist]
    return dict(Counter(data_process))


""" Input: dos listas de diccionarios
    Output: una lista de diccionarios
    si una clave de dicc2 existe en dicc1:
        sumar valor de dicc2 a clave de dicc1
    si no existe:
        agregar clave:valor de dicc2 a dicc1"""
def reducer_CounterTags(dicc1, dicc2):
    for key, value in dicc2.items():
        if key in dicc1.keys():
            dicc1[key] += value
        else:
            dicc1[key] = value
    return dicc1


if '__main__' == __name__:
    try:
        path = str(pathlib.Path().absolute()) + '/../..' + \
            config('dataset_path') + 'posts.xml'
        tree_xml = ET.parse(path)
    except FileNotFoundError as e:
        logger.error(f"top10tags - Archivo de datos no encontrado en la ruta: {e}")
        sys.exit(1)
    except ET.ParseError as e:
        logger.error(f"top10tags - Error al parsear el archivo: {path}")
        logger.error(e.msg)
        sys.exit(1)
    data_chunks = divide_chunks(tree_xml.getroot(), 100)
    logger.info(f'top10tags - Division de datos en chunks del archivo {path}')

    logger.info('top10tags - Inicio del procesamiento')
    # llamada a la funcion mapper principal
    mapped = list(map(mapper, data_chunks))
    # llamada a la funcion reduce principal
    mapped = reduce(reducer_CounterTags, mapped)
    # ordenar los valores, obviamente no en diccionarios
    mapped = sorted(mapped.items(), key=lambda x: x[1], reverse=True)[:10]

    logger.info('top10tags - Fin del procesamiento')
    path_download = str(pathlib.Path().absolute()) + \
        '/../..' + config('files_path') + 'top10tagsA.txt'
    try:
        with open(path_download, 'w') as f:
            for i in range(len(mapped)):
                f.write(str(i + 1) + ': ' +
                        mapped[i][0] + ' ' + str(mapped[i][1]) + '\n')
    except Exception as e:
        logger.error(f'top10tags - Error al escribir archivo: {path_download}, fin del programa')
        sys.exit(1)
    logger.info(f'top10tags - Archivo de salida: {path_download}')
