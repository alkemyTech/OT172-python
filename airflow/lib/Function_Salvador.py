from datetime import datetime, date
import pandas as pd
import os 
import logging

def Normalizacion(N_Archivo):
    try:
        ruta_absoluta = os.getcwd()
        ruta_csv = ruta_absoluta + N_Archivo
        ruta_csv2 = ruta_absoluta + "/dataset/codigos_postales.csv"
        DF_Salvador = pd.read_csv(ruta_csv)
        DF_Codigos = pd.read_csv(ruta_csv2)
        logging.info(DF_Salvador.head())
        logging.info(DF_Codigos.head())
    except Exception as e:
        logging.error(e)
    try:
        DF_Salvador['university'] = DF_Salvador['university'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['career'] = DF_Salvador['career'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['first_name'] = DF_Salvador['first_name'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['last_name'] = DF_Salvador['last_name'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['location'] = DF_Salvador['location'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['email'] = DF_Salvador['email'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador['gender'] = DF_Salvador['gender'].apply(lambda x: 'male' if x == 'M' else 'female')
        def Months(Date_in):
            meses = {
                    'Jan': "01",
                    'Feb': "02",
                    'Mar': "03",
                    'Apr': "04",
                    'May': "05",
                    'Jun': "06",
                    'Jul': "07",
                    'Aug': "08",
                    'Sep': "09",
                    'Oct': "10",
                    'Nov': "11",
                    'Dec': "12"
                    }
            fecha = Date_in.split("-")
            dia =  fecha[0]
            mes =  fecha[1]
            a = fecha[2]
            mes_salida = str(meses[mes])
            Date = dia + "-" +  mes_salida + "-" + a
            return Date
        DF_Salvador['inscription_date'] = DF_Salvador['inscription_date'].apply(Months)
        DF_Salvador['inscription_date'] = pd.to_datetime(DF_Salvador['inscription_date'], format="%d-%m-%y")        
        DF_Salvador['age'] = DF_Salvador['age'].apply(Months)
        def B_date(Date):
            Date_b = datetime.strptime(str(Date), "%d-%m-%y").date()
            Date_t = date.today()
            age = Date_t.year - Date_b.year - ((Date_t.month,  Date_t.day) <(Date_b.month,  Date_b.day))
            if age < 0:
                age = 101 + age 
            return age
        DF_Salvador['age'] = DF_Salvador['age'].apply(B_date)
        DF_Salvador['location'] = DF_Salvador['location'].str.strip().str.lower().str.replace("-", " ").str.replace("_", " ")
        DF_Salvador.to_csv(ruta_absoluta + "/files/Uni_Salvador2.csv", index=None, encoding="utf-8")
    except Exception as e:
        logging.error(e)
    return None

if __name__ == '__main__':
    Normalizacion("/files/Uni_Comahue.csv")