#from bigdata.dataGroupA.relationNpalabrasPuntaje import mapped
from decouple import config
from functools import reduce, partial
from datetime import datetime 
from statistics import mean
import xml.etree.ElementTree as ET
import pathlib
import time 
import re

def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def get_pId_Date_comment(xml):
    return {str(xml.attrib['PostId']) : [
        str_to_datetime(xml.attrib['CreationDate'])]}

def get_Id_Date_post(xml):
    return {str(xml.attrib['Id']) : [
        str_to_datetime(xml.attrib['CreationDate'])]}

# modificarla
def str_to_datetime(date_string):
    date_string = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%f')
    return date_string

def unir_pId_duplicados(dic1, dic2):
    for key, value in dic2.items():
        if key in dic1.keys():
            dic1[key] += value
        else:
            dic1[key] = value
    return dic1


# return un diccionario
def mapped_comments(c_chunk):
    mappedOne = list(map(get_pId_Date_comment, c_chunk))
    mappedOne = reduce(unir_pId_duplicados, mappedOne)
    return mappedOne

def mapped_posts(p_chunk):
    mappedOne = list(map(get_Id_Date_post, p_chunk))
    return mappedOne


def only_whit_comments(Onedicc, comments_dicc):
    for key, value in Onedicc.items():
        if key in comments_dicc.keys():
            Onedicc[key].append(comments_dicc[key])
        else:
            return None
    return Onedicc
            

def obtener_tiempo_promedio(Onedicc):
    # para el unico elemento del diccionario
    for key, value in Onedicc.items():
        aux_lambda = lambda x : (x - Onedicc[key][0]).total_seconds()
        Onedicc[key][1] = list(map(aux_lambda,Onedicc[key][1]))
        #eliminar el date post
        Onedicc[key].pop(0)
        #aplanar
        Onedicc[key] = Onedicc[key][0]
    
    # para el unico elemento del diccionario
    for key,value in Onedicc.items():
        Onedicc[key] = round(mean(Onedicc[key]),2)
        Onedicc[key] = time.strftime("%H:%M:%S", time.gmtime(Onedicc[key]))

    return Onedicc

        

if '__main__' == __name__:
    path_comments = str(pathlib.Path().absolute()) +'/../..' + config('dataset_path') + 'comments.xml'
    path_posts = str(pathlib.Path().absolute()) +'/../..' + config('dataset_path') + 'posts.xml'

    tree_comments = ET.parse(path_comments)
    tree_posts = ET.parse(path_posts)

    chunks_comments = divide_chunks(tree_comments.getroot(), 50)
    chunks_posts = divide_chunks(tree_posts.getroot(), 50)

    # COMENTARIOS
    comments = list(map(mapped_comments, chunks_comments))
    """ llegado a este punto tenes un solo diccionario 
    {'pId': [lista de fechas de comentarios],
    'pId': [lista de fechas de comentarios],
    'pId': [lista de fechas de comentarios]}"""
    comments = reduce(unir_pId_duplicados, comments)

    # POSTS
    posts = list(map(mapped_posts, chunks_posts))
    posts = reduce(lambda x, y: x + y, posts)
    # COMBINAR LOS DICCIONARIOS
    fun_lambda = lambda x : only_whit_comments(x,comments)
    posts = list(map(fun_lambda,posts))
    posts = list(filter(None, posts))
    # OBTENER EL PROMEDIO DE TIEMPO DE RESPUESTA POR POST
    posts = list(map(obtener_tiempo_promedio, posts))
    
    print(posts[:10])



