"""
Utilizar MapReduce para el grupo de datos E

Parte 2:
        * Relación entre cantidad de respuestas y sus visitas.
"""

from functools import reduce
import xml.etree.ElementTree as ET
import logging
import logging.config
import time
import os

ruta_base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
try:
    logging.config.fileConfig(f'{ruta_base}/data_group_E/logging.cfg')
    
    # create logger
    logger = logging.getLogger('Relacion_respuestas_visitas')
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
        for i in range(0, len(iterable), len_of_chunk):
            yield iterable[i:i +len_of_chunk]
        
    except TypeError as e:
        logger.error(f"Ocurrió una excepción identificada: {e}")

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


def obt_views_answer(data):
    """
    Obtiene los datos de los atributos 'ViewCount' y 'AnswerCout'
    Arg: Datos obtenidos del repositorio
    Return: retorna los datos en forma de enteros, de view y answer
    """
    try:
        try:
            answer_count = int(data.attrib['AnswerCount'])
            view_count = int(data.attrib['ViewCount'])
            return view_count, answer_count
        except:
            return        
    except Exception as e:
        logger.error(f'Error al obtener las visitas y respuestas {e}')

def reducir_views_answer(data1, data2):
    """
    Reduce los resultados sumandolos
    Arg: Recibe dos tuplas que se suman entre si. Cada una tiene 2 enteros.
    Return: Retorna una tupla con la suma de los dos primero numero y los dos segundos
    """
    v = data1[0]+data2[0]
    a = data1[1]+data2[1]
    data1 = (v , a)
    return data1

def mapper(data):
    """
    Funcion que hace el mapeo para obtener las visitas y las respuestas.
    Luego hace un reduce de esos datos.
    Arg: Recibe el data_chuks
    return: Retorna una lista de tuplas ya reducidas.
    """
    view_answer = list(map(obt_views_answer, data))
    view_answer = list(filter(None, view_answer))
    try:
        reducido = reduce(reducir_views_answer, view_answer)
    except:
        return
    return reducido

def respuestas_y_visitas():
    """
    Funcion principal encargada controlar la ejecucion del programa
    Return: Retorna el resultado final de visitas y respuestas.
    """
    try:
        data = obtener_datos()
        data_chuncks = chunk_data(data, 50)
        logger.info('Datos separados en partes con exito')
        mapped = list(map(mapper, data_chuncks))
        mapped = list(filter(None, mapped))
        resultado = reduce(reducir_views_answer, mapped)
        logger.info('Cantidad total de views y answer obtenidas con exito')
        return resultado
    except Exception as e:
        logger.error(f'Errro en la ejecucion de respuestas_y_visitas(). {e} ')

if __name__=="__main__":
    """
    Ejecuta la funcion principal: respuestas_y_visitas()
    Calula el tiempo desde que comienza la ejecucion hasta que termina.
    """
    time_start = time.time()
    logger.info('Comieza la ejecucion del programa')
    relacion = respuestas_y_visitas()
    logger.info('Fin del procesamiento de datos')
    time_end = time.time()
    logger.info(f'Tiempo para procesar los datos: {time_end - time_start}')

    logger.info(f'Se obtuvieron {relacion[0]} visitas y {relacion[1]} respuestas en los datos analizados')
    a = relacion[0]/relacion[1]
    logger.info(f'En ralacion, se genera 1 respuesta cada {round(a,2)} visitas')