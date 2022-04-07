# Esta funcion guarda el archivo .txt en S3
# Obtine los parametros filename y key de archivo .yml

import os
from os import path
import logging
from decouple import config

from airflow.hooks.S3_hook import S3Hook


BUCKET_NAME = config('BUCKET_NAME')

ruta_base = path.abspath(path.join(path.dirname(__file__), ".."))
ruta_files = path.abspath(path.join(ruta_base, 'files'))


def upload(filename, key):
    """ upload file to s3 """
    try:
        hook = S3Hook('s3_conn')
        hook.load_file(
            filename=filename,
            key=key,
            bucket_name=BUCKET_NAME,
            replace=True)
        logging.info('The file was saved')
    except:
        logging.error('Error to load file or already exists')
