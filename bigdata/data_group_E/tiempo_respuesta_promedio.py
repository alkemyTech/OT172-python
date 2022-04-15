"""

Utilizar MapReduce para el grupo de datos E

* Del ranking de los primeros 0-100 por score, tomar el tiempo 
  de respuesta promedio e informar un único valor.
"""

import os
import sys
sys.path.insert(1, os.path.abspath("C:/Users\Lucyfer\Documents\Fernando\Alkemy\Aceleracion\OT172\OT172-python/bigdata"))
from functools import reduce
from typing import Counter
import xml.etree.ElementTree as ET
import re
import logging
import logging.config
import time
import datetime
from lib.chunkify import chunk_data

ruta_base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
try:
    logging.config.fileConfig(f'{ruta_base}/data_group_E/logging.cfg')
    # create logger
    logger = logging.getLogger('Tiempo_Respuesta_promedio')
    """
    # application code for logger
    logger.debug('debug message')
    logger.info('info message')
    logger.warning('warn message')
    logger.error('error message')
    logger.critical('critical message')
    """
except KeyError as e:
   print('No se encontro el archivo logging.cfg en el path especificado.')
   raise e
except FileNotFoundError as e:
    print(f'La ruta el directorio es incorrecta. {e}')
    raise e

def obtener_datos():
    """
    Lee los datos desde un archivo externo
    Return: retorna los datos que seran procesados luego
    """
    try:
        post = ET.parse(f"{ruta_base}/data_set/112010 Meta Stack Overflow/posts.xml")
        data_post = post.getroot()
        logger.info('Datos obtenidos con exito.')
        return data_post
    except FileNotFoundError as e:
        logger.error("Archivo de datos no encontrado en la ruta.")
        raise FileNotFoundError(f"Error al obtener los datos: {e} ")
    except Exception as e:
        logger.error(e)
        raise e

def obt_score(data):
    """
   
    """
    try:
        score = int(data.attrib['Score'])
        post_type_id = data.attrib[ 'PostTypeId']
        try:
            ParentID = data.attrib['ParentId']
        except:
            ParentID = None
        try:
            AcceptedAnswerId = data.attrib['AcceptedAnswerId']
        except:
            AcceptedAnswerId = None
        CreationDate = data.attrib['CreationDate']
        return score, post_type_id, ParentID, AcceptedAnswerId, CreationDate
    except Exception as e:
        logger.error(f'Error al obtener las visitas y respuestas {e}')
        raise e

def mapper(data):
    """
    Arg: Recibe el data_chuks
    return: 
    """
    view_answer = list(map(obt_score, data))
    #reducido = reduce(reducir_views_answer, view_answer)
    return view_answer

def tiempo_respuesta_promedio():
    """
    Funcion principal encargada controlar la ejecucion del programa
    Return: Retorna el resultado final.
    """
    try:
        data = obtener_datos()
        data_chuncks = chunk_data(data, 50)
        logger.info('Datos separados en partes con exito')
        mapped = list(map(mapper, data_chuncks))

        return mapped
     
        
    except Exception as e:
        logger.error(f'Errro en la ejecucion de tiempo_respuesta_promedio(). {e} ')

if __name__=="__main__":
    """
    Ejecuta la funcion principal: tiempo_respuesta_promedio()
    Calula el tiempo desde que comienza la ejecucion hasta que termina.
    """
    time_start = time.time()
    logger.info('Comieza la ejecucion del programa')
    tiempo_promedio = tiempo_respuesta_promedio()
    logger.info('Fin del procesamiento de datos')
    time_end = time.time()
    logger.info(f'Tiempo para procesar los datso: {time_end - time_start}')

    '''
    logger.info(f'Se obtuvieron {relacion[0]} visitas y {relacion[1]} respuestas en los datos analizados')
    a = relacion[0]/relacion[1]
    logger.info(f'En ralacion, se genera 1 respuesta cada {round(a,2)} visitas')
    '''