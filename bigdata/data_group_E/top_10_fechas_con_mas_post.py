"""

Utilizar MapReduce para el grupo de datos E

* Top 10 fechas con mayor cantidad de post creados

"""

from functools import reduce
from typing import Counter
import xml.etree.ElementTree as ET
import logging
import logging.config
import time
import datetime
import os, sys


ruta_base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
#ruta_include = path.abspath(path.join(ruta_base, 'include'))
try:
    logging.config.fileConfig(f'{ruta_base}/data_group_E/logging.cfg')
    
    # create logger
    logger = logging.getLogger('top_10_fechas_con_mas_post')
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
    



def chunk_data(iterable, len_of_chunk):
    """ 
    Se divide la data en partes para poder trabajarla
    arg: iterable: lista de datos obtenida
         len_of_chunk: cantidad de partes en las que se dividira la lista
    retunr: Retorna la lista dividida en partes
    """
    try:
        if len_of_chunk < 0:
            raise TypeError('El numero de len_of_chunk debe ser mayor a 0')
        resultado = [iterable[i:i + len_of_chunk] for i in range(0, len(iterable), len_of_chunk)]
        logger.info('Datos separados en partes con exito')
        return resultado
    except TypeError as e:
        logger.error(f"Ocurrió una excepción identificada: {e}")
    


def obtener_fechas(data):
    """
    Obtiene las fechas de creacion de los post desde: CreationDate.
    Luego se formatea la fecha para que retorne Año, Mes, Dia
    Return: Retorna las fechas formateadas.
    """
    try:
        fechas = data.attrib['CreationDate']
        fecha_formateada = datetime.datetime.strptime(fechas, '%Y-%m-%dT%H:%M:%S.%f')
        fecha_formateada = fecha_formateada.strftime('%Y-%m-%d')
        
        return fecha_formateada
    except Exception as e:
        logger.error(f'Error en la obtencion de las fechas: {e}')

def suma_fechas(data1, data2):
    """
    Se realiza un update a cada Counter para sumar la cantidad de post que hay por fecha
    """
    data1.update(data2)
    return data1

def mapper(data):
    """
    Primero se obtine la lista de fechas en las que hay un post.
    Luego, mediante el Counter, se devuelve la suma de todas las fechas que tienen post

    Arg: data: Info obtenida y dividida partes
    Return: Retorna todas las fechas, con la cantidad de post que se hicieron en esa fecha

    """
    fechas_mapeadas = list(map(obtener_fechas, data))
    counter_fecha = Counter(fechas_mapeadas)
    return counter_fecha


def top_10_post_por_fecha():
    """
    Funcion principal: ejecuta la obtencion de la data.
    Ejecuta al divicion de la data en 50 partes, medianta data_chuncks.
    Se mapea el data_chuncks para procesar los datos.
    Se hace un reduce para sumar los datos y obtener el top_10
    Return: Retorna el top 10 de fechas con mas post
    """
    try:
        data = obtener_datos()
        data_chuncks = chunk_data(data, 50)
        mapped = list(map(mapper, data_chuncks))
        logger.info('Fechas obtenidas y formateadas con exito')
        top_10 = reduce(suma_fechas, mapped).most_common(10)
        logger.info('Cantidad total de post por fechas obtenidos y seleccionado el top 10')
        return top_10
    except Exception as e:
        logger.error(f'Errro en la ejecucion de top_10_post_por_fecha. {e} ')


if __name__=="__main__":
    """
    Ejecuta la funcion principal: top_10_post_por_fecha()
    Calula el tiempo deade que comienza la ejecucion hasta que termina.
    """
    time_start = time.time()
    logger.info('Comiena la ejecucion del programa')
    top = top_10_post_por_fecha()
    logger.info('Fin de la ejecucion')
    time_end = time.time()

    logger.info(f'El top 10 fechas con mayor cantidad de post es:\n')
    for i in top:
        logger.info(f'En la fecha: {i[0]} se crearon => {i[1]} posts')
    
    logger.info(f'Tiempo para procesar los datos: {time_end - time_start}')

