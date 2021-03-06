from decouple import config
import xml.etree.ElementTree as ET
from functools import reduce
import pathlib
import re
import logging
import logging.config
import pathlib
import sys


logging.config.fileConfig(f'{pathlib.Path(__file__).parent.absolute()}/logging.cfg')
logger = logging.getLogger('relationWordsScore')


def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


""" Input: un xml formateado
    Output: un diccionario con los datos del xml
        id, len_words, score, relation
    Aqui esta el fuerte de todas las funciones, formateando y limpiando
    los campos del xml que necesitamos ademas de calcular la relacion"""
def body_score(xml):
    words = re.findall(
        r'(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',
        xml.attrib['Body'])
    score = int(xml.attrib['Score'])
    return {
        'id': xml.attrib['Id'],
        'len_words': len(words),
        'score': score,
        'relation': abs(len(words) / (score, 1)[score == 0])
    }


""" Input: lista de xml
    Output: lista de diccionarios con id, len_words, score, relation
    solo llama a la funcion body_score para un map"""
def mapped(data_chunk):
    return list(map(body_score, data_chunk))


if '__main__' == __name__:
    try:
        path = str(pathlib.Path().absolute()) + '/../..' + \
            config('dataset_path') + 'posts.xml'
        tree_xml = ET.parse(path)
    except FileNotFoundError as e:
        logger.error(f'relationWordsScore - Archivo no encontrado: {path}')
        logger.error(f'relationWordsScore - Error: {e}')
        sys.exit(1)
    data_chunks = divide_chunks(tree_xml.getroot(), 100)
    logger.info(f'relationWordsScore - Division en chunks de datos: {path}')

    logger.info(f'relationWordsScore - Mapeo de datos')
    # llamada a la funcion mapper principal
    mapped = list(map(mapped, data_chunks))
    # aplanar lista con reduce
    mapped = reduce(lambda x, y: x + y, mapped)
    logger.info(f'relationWordsScore - Aplanado de datos, datos listos')

    try:
        path_download = str(pathlib.Path().absolute()) + '/../..' + \
            config('files_path') + 'relationWordsScoreA.txt'
        with open(path_download, 'w') as f:
            for item in mapped:
                f.write('PostID: ' +
                        str(item['id']) +
                        ' Relation: ' +
                        str(item['relation']) +
                        '\n')
    except Exception as e:
        logger.error(f'relationWordsScore - Error al escribir en: {path_download}')
        logger.error(f'relationWordsScore - Error: {e}')
        sys.exit(1)
    logger.info(f'relationWordsScore - Archivo creado: {path_download}')
