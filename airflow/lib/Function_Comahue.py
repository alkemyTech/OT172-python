from datetime import datetime, date
import pandas as pd
import os 
import logging

def Normalizacion(N_Archivo):
    try:
        ruta_absoluta = os.getcwd()
        ruta_csv = ruta_absoluta + N_Archivo
        ruta_csv2 = ruta_absoluta + "/dataset/codigos_postales.csv"
        DF_Nacional = pd.read_csv(ruta_csv)
        DF_Codigos = pd.read_csv(ruta_csv2)
        logging.info(DF_Nacional.head())
        logging.info(DF_Codigos.head())
    except Exception as e:
        logging.error(e)
    try:
        DF_Nacional['university'] = DF_Nacional['university'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['career'] = DF_Nacional['career'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['first_name'] = DF_Nacional['first_name'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['last_name'] = DF_Nacional['last_name'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['location'] = DF_Nacional['location'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['email'] = DF_Nacional['email'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Nacional['gender'] = DF_Nacional['gender'].apply(lambda x: 'male' if x == 'M' else 'female')
        DF_Nacional['inscription_date'] = DF_Nacional['inscription_date'].str.replace("-", "/")
        DF_Nacional['inscription_date'] = pd.to_datetime(DF_Nacional['inscription_date'], format="%Y/%m/%d")
        DF_Nacional['inscription_date'] = DF_Nacional['inscription_date'].str.replace("/", "-")
        DF_Nacional['age'] = DF_Nacional['age'].str.replace("-", "/")
        def B_date(Date):
            Date_b = datetime.strptime(str(Date), "%Y/%m/%d").date()
            Date_t = date.today()
            return Date_t.year - Date_b.year - ((Date_t.month,  Date_t.day) <(Date_b.month,  Date_b.day))
        DF_Nacional['age'] = DF_Nacional['age'].apply(B_date)
        DF_Codigos['localidad'] = DF_Codigos['localidad'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        dic = dict(zip(DF_Codigos['codigo_postal'], DF_Codigos['localidad']))
        def location(codigo):
            location = dic[codigo]
            return location
        DF_Nacional['location'] = DF_Nacional['postal_code'].apply(location)
        DF_Nacional.to_csv(ruta_absoluta + "/files/Uni_Comahue.txt", index=None, sep='\t', encoding="utf-8")
    except Exception as e:
        logging.error(e)
    return None

if __name__ == '__main__':
    Normalizacion("/files/Uni_Comahue.csv")