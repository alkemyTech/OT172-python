import pandas as pd
import numpy as np
import pathlib
from datetime import datetime
from dateutil.relativedelta import relativedelta
from difflib import SequenceMatcher as SM


# Input: Ruta + nombre del Archivo.csv a normalizar(ET_Univ_del_Cine.csv)
#           Ruta + nombre del Archivo2.csv con los postal_code y locations
#           Ruta de la carpeta donde se guardara el .csv en formato .txt
# Output: .txt en la ruta del tercer parametro

def normalize_df_Uni_Cines(path_df, path_dfmerge, path_download):
    def dateparse(x): return datetime.strptime(x, '%d-%m-%Y')

    df = pd.read_csv(path_df, dtype={'age': int},
                     parse_dates=['inscription_dates', 'birth_dates'],
                     date_parser=dateparse)

    df.columns = ['university', 'career', 'inscription_date', 'full_name',
                  'gender', 'age', 'location', 'email']

    dfmerge = pd.read_csv(path_dfmerge)
    dfmerge.columns = ['postal_code', 'location']
    dfmerge['location'] = dfmerge['location'].apply(lambda x: x.lower())

    df['first_name'] = np.nan
    df['last_name'] = np.nan

    for row_i in range(len(df)):
        # University
        uni = df.loc[row_i, 'university']
        df.loc[row_i, 'university'] = uni.lower().replace("-", " ").strip()

        # Career
        carr = df.loc[row_i, 'career']
        df.loc[row_i, 'career'] = carr.lower().replace("-", " ").strip()

        # inscription_date
        df.loc[row_i, 'inscription_date'] = str(
            df.loc[row_i, 'inscription_date'])

        # fist name
        # lista de abreviaciones mas usadas
        list_abrev = [
            'dr',
            'dra',
            'ms',
            'mrs',
            'ing',
            'lic',
            'ph.d',
            'mtro',
            'arq']
        # funcion auxiliar, comprueba que si existe alguna abreviacion de
        # profesion

        def match_check(elem, list_c):
            for i in list_c:
                return True if SM(None, elem.lower(), i).ratio() > 0.6 else ""
            return False

        fname = df.loc[row_i, 'full_name'].lower().split("-")
        fname = fname[1:] if match_check(fname[0], list_abrev) else fname
        df.loc[row_i, 'first_name'] = fname[0]

        # last name
        df.loc[row_i, 'last_name'] = fname[-1]

        # gender
        gen = df.loc[row_i, 'gender']
        df.loc[row_i, 'gender'] = (
            (np.nan, 'female')[gen == 'F'], 'male')[gen == 'M']

        # age, ya esta en formato int64
        age = df.loc[row_i, 'age']
        df.loc[row_i, 'age'] = relativedelta(datetime.now(), age).years

        # location
        loc = df.loc[row_i, 'location']
        df.loc[row_i, 'location'] = loc.lower().replace("-", " ").strip()

        # email, no pueden tener espacios pero si guiones entre los caracteres
        email = df.loc[row_i, 'email']
        df.loc[row_i, 'email'] = email.lower().strip("-").replace(" ", "")

    # postal_code, realizaremos un left join
    df = df.merge(
        dfmerge,
        on='location',
        how='left').drop(
        ['full_name'],
        axis=1)

    # importar a una ruta especifica
    df.to_csv(path_download + '/Uni_Del_Cines.txt', sep="|")


if __name__ == '__main__':
    pathOne = str(pathlib.Path().absolute()) + '/../files/'
    pathTwo = str(pathlib.Path().absolute()) + '/../dataset/'
    normalize_df_Uni_Cines(pathOne + 'ET_Univ_del_Cine.csv',
                           pathTwo + 'codigos_postales.csv',
                           pathOne)
