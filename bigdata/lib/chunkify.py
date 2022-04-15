

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
        
        return resultado
    except TypeError as e:
        print(f"Ocurrió una excepción identificada: {e}")
'''
def chunkify(iterable, len_of_chunk):
    for i in range(0, len(iterable), len_of_chunk):
        yield iterable[i:i +len_of_chunk]
'''