"""
Configuracion del archivo .cfg para el logging

Utilizar MapReduce para el grupo de datos E
* Top 10 fechas con mayor cantidad de post creados

* Relación entre cantidad de respuestas y sus visitas.

* Del ranking de los primeros 0-100 por score, tomar el tiempo de respuesta promedio e informar un único valor.
"""



import logging
import logging.config

logging.config.fileConfig('OT172-python/bigdata/map_reduce/logging.cfg')

# create logger
logger = logging.getLogger(__name__)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')



