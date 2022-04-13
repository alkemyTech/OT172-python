"""
Configuracion del archivo .cfg para el logging

Utilizar MapReduce para el grupo de datos E
* Top 10 fechas con mayor cantidad de post creados

* Relación entre cantidad de respuestas y sus visitas.

* Del ranking de los primeros 0-100 por score, tomar el tiempo de respuesta promedio e informar un único valor.
"""

from functools import reduce
from typing import Counter
import xml.etree.ElementTree as ET
import re
import logging
import logging.config
import time
import datetime


logging.config.fileConfig('OT172-python/bigdata/map_reduce/logging.cfg')
# create logger
logger = logging.getLogger(__name__)
"""
# application code for logger
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')
"""

def obtener_datos():
    """
    Se obtienen los datos que seran procesados.
    """
    post = ET.parse(r"112010 Meta Stack Overflow\posts.xml")
    data_post = post.getroot()
    return data_post


def chunk_data(iterable, len_of_chunk):
    """ 
    Se divide la data en partes para poder trabajarla
    """
    return [iterable[i:i + len_of_chunk] for i in range(0, len(iterable), len_of_chunk)]


def obtener_fechas(data):
    """
    Obtiene las fechas de creacion de los post desde: CreationDate.
    Luego se formatea la fecha para que retorne Año, Mes, Dia
    """
    fechas = data.attrib['CreationDate']
    fecha_formateada = datetime.datetime.strptime(fechas, '%Y-%m-%dT%H:%M:%S.%f')
    fecha_formateada = fecha_formateada.strftime('%Y-%m-%d')
    return fecha_formateada

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
    Retorna todas las fechas, con la cantidad que de post que se hicieron en esa fecha
    """
    fechas_mapeadas = list(map(obtener_fechas, data))
    counter_fecha = Counter(fechas_mapeadas)
    return counter_fecha


def top_10_post_por_fecha():
    """
    Funcion principal: ejecuta la obtencion de la data.
    Ejecuta al divicion de la data en 50 partes, medianta data_chuncks.
    Se mapea el data_chuncks para procesar los datso.
    Se hace un reduce para sumar los datos y obtener el top_10
    """
    data = obtener_datos()
    data_chuncks = chunk_data(data, 50)
    mapped = list(map(mapper, data_chuncks))
    top_10 = reduce(suma_fechas, mapped).most_common(10)
    

    return top_10


if __name__=="__main__":
    """
    Ejecuta la funcion principal: top_10_post_por_fecha()
    Calula el tiempo deade que comienza la ejecucion hasta que termina.
    """
    time_start = time.time()
    top_10_post_por_fecha()
    time_end = time.time()

    print(f'El top 10 fechas con mayor cantidad de post es:\n')
    for i in top_10_post_por_fecha():
        print(f'Fecha: {i[0]} se crearon => {i[1]} posts')
    
    print(f'Tiempo para procesar los datso: {time_end - time_start}')

