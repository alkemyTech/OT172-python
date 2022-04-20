#from bigdata.dataGroupA.relationNpalabrasPuntaje import mapped
from decouple import config
from functools import reduce, partial
from datetime import datetime
from statistics import mean
import xml.etree.ElementTree as ET
import logging
import logging.config
import pathlib
import sys
import time
import re


logging.config.fileConfig(f'{pathlib.Path(__file__).parent.absolute()}/logging.cfg')
logger = logging.getLogger('averageResponse')

def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def get_pId_Date_comment(xml):
    return {str(xml.attrib['PostId']): [
        str_to_datetime(xml.attrib['CreationDate'])]}


def get_Id_Date_post(xml):
    return {str(xml.attrib['Id']): [
        str_to_datetime(xml.attrib['CreationDate'])]}


def str_to_datetime(date_string):
    date_string = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%f')
    return date_string


""" Input: Diccionario1, Diccionario2
    Output: pseudo Union de los dos diccionarios

    si el postId de un comentario de dic2 esta
    en dic1 entonces agregar el Date de comentario
    al dic1, caso contrario crear en dic1 postId: Date"""
def group_dates_with_postId(dic1, dic2):
    for key, value in dic2.items():
        if key in dic1.keys():
            dic1[key] += value
        else:
            dic1[key] = value
    return dic1


""" Input: lista de xml
    Ouput: lista de diccionarios con formato
        {postId: [lista de fechas de comentarios a ese postId]}"""
def mapped_comments(c_chunk):
    # Obtener los postId y fecha de creacion de cada comentario
    mappedOne = list(map(get_pId_Date_comment, c_chunk))
    # agrupar las fechas de los comentarios por postId
    mappedOne = reduce(group_dates_with_postId, mappedOne)
    return mappedOne


""" Input: lista de xml
    Ouput: lista de diccionarios con formato
        {Id: [lista con solamente su fecha de creacion]}"""
def mapped_posts(p_chunk):
    return list(map(get_Id_Date_post, p_chunk))


""" Input: Diccionario con un solo par clave valor
           Diccionario de los comentarios agrupados por postId
           ej: {postId: [lista de fechas de comentarios a ese postId]}
    Output: El diccionario con solo un par clave valor donde en valor
            se le agrego la lista de comentarios si esa clave aparecia
            como postId en el diccionario de comentarios
            Caso contrario retorna el diccionario con la misma clave,
            pero con valor None"""
def only_whit_comments(Onedicc, comments_dicc):
    for key, value in Onedicc.items():
        if key in comments_dicc.keys():
            Onedicc[key].append(comments_dicc[key])
        else:
            return None
    return Onedicc


""" Input: un diccionario con un solo par de clave valor tipo:
    {Id: [[date_creacion_Post],[dates_comentarios]]}
    Output: un diccionario con un solo par de clave valor tipo:
    {Id: time -> indicando el tiempo promedio de respuesta en horas}"""
def calculed_average(Onedicc):
    # para el unico elemento del diccionario
    for key, value in Onedicc.items():
        def aux_lambda(x): return (x - Onedicc[key][0]).total_seconds()
        # obtener diferencia en segundos entre
        # fecha de creacion del post y fecha de creacion del comentario
        Onedicc[key][1] = list(map(aux_lambda, Onedicc[key][1]))
        # eliminar el date post
        Onedicc[key].pop(0)
        # aplanar
        Onedicc[key] = Onedicc[key][0]

    # para el unico elemento del diccionario
    for key, value in Onedicc.items():
        # calcular el promedio
        Onedicc[key] = round(mean(Onedicc[key]), 2)
        # formatear a horas
        Onedicc[key] = time.strftime("%H:%M:%S", time.gmtime(Onedicc[key]))

    return Onedicc


"""
    Manejamos dos .xml, ya que el objetivo es:
        obtener el tiempo de respuesta promedio de cada post
        sea respuesta aceptada o no.
"""
if '__main__' == __name__:
    ##### PREPARNDO LOS ARCHIVOS #####
    try:
        path_comments = str(pathlib.Path().absolute()) + \
            '/../..' + config('dataset_path') + 'comments.xml'
        path_posts = str(pathlib.Path().absolute()) + '/../..' + \
            config('dataset_path') + 'posts.xml'

        tree_comments = ET.parse(path_comments)
        tree_posts = ET.parse(path_posts)
    except FileNotFoundError as e:
        logger.error(f'averageResponse-No existe el archivo :\n{e.filename}')
        sys.exit(1)
    except ET.ParseError as e:
        logger.error(f'averageResponse-Error al parsear un archivo:\n{e.filename}')
        logger.error(f'averageResponse-Error: {e}')
        sys.exit(1)
    except Exception as e:
        logger.error(e)
        sys.exit(1)

    chunks_comments = divide_chunks(tree_comments.getroot(), 50)
    logger.info(f'averageResponse-Division chunks de comentarios listo')
    chunks_posts = divide_chunks(tree_posts.getroot(), 50)
    logger.info(f'averageResponse-Division de chunks de posts listo')

    ##### COMENTARIOS #####
    logger.info('averageResponse-Procesando comentarios...')
    # llamada a la funcion mapper de comentarios
    comments = list(map(mapped_comments, chunks_comments))
    # agrupar las fechas de comentarios por postId
    comments = reduce(group_dates_with_postId, comments)
    """ llegado a este punto tenes un solo diccionario en comments
    {'pId': [lista de fechas de comentarios],
    'pId': [lista de fechas de comentarios],
    'pId': [lista de fechas de comentarios]}"""
    logger.info('averageResponse-Comentarios procesados')

    ##### POSTS #####
    logger.info('averageResponse-Procesando posts...')
    # llamada a la funcion mapper de posts
    posts = list(map(mapped_posts, chunks_posts))
    # aplanar la lista de diccionarios
    posts = reduce(lambda x, y: x + y, posts)
    logger.info('averageResponse-Posts procesados')

    ##### COMBINAR LOS POSTS Y COMENTARIOS #####
    logger.info('averageResponse-Combinando posts y comentarios...')
    # basicamente agregar a los posts la lista de fechas de comentarios
    # si es que tienen comentarios
    def aux_lambda(x): return only_whit_comments(x, comments)
    posts = list(map(aux_lambda, posts))
    # limpiar los post que no tienen comentarios
    posts = list(filter(None, posts))
    logger.info('averageResponse-Combinacion finalizada')

    ##### OBTENER EL PROMEDIO DE TIEMPO DE RESPUESTA POR POST #####
    logger.info('averageResponse-Obteniendo promedio de tiempo de respuestas...')
    posts = list(map(calculed_average, posts))

    try:
        path_download = str(pathlib.Path().absolute()) + '/../..' + \
            config('files_path') + 'averageResponsePostA.txt'
        with open(path_download, 'w') as f:
            for dict in posts:
                for key, value in dict.items():
                    f.write('PostId: ' + key + ' - avg_comments: ' + value + '\n')
    except Exception as e:
        logger.error(f'averageResponse-Error al escribir el archivo:\n{e.filename}')
        logger.error(f'averageResponse-Error: {e}')
        sys.exit(1)
    logger.info('averageResponse-Proceso finalizado')