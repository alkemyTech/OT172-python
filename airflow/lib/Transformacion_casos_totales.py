# Data transformation functions


# normalize characters
# removes spaces at the beginning or end of strings, hyphens and
# convert words to lower case


from lib.functios import extraction


def normalize_characters(column):
    """
    Args: a df column

    output: a df column without special characters

    The function takes the string values of the 
    column of a df and normalizes the special characters
    """
    try:
        column = column.apply(lambda x: str(
            x).replace(' \W'+'*'+'\W', '\W'+'*'+'\W'))
        column = column.apply(lambda x: str(
            x).replace('\W'+'*'+'\W ', '\W'+'*'+'\W'))
        column = column.apply(lambda x: str(x).replace('-', ' '))
        column = column.apply(lambda x: str(x).replace('_', ' '))
        column = column.apply(lambda x: x.lower())
    except:
        logging.ERROR('normalizing failed')
    return column

# transform_df
# transform the data in a pd.DataFrame format

# lines 1-3: apply the function previously defined
# line : conditional that changes m and f to male and female, in the gender column
# lines 5-6: The string contained in inscription date is passed to date format
# and the required format is assigned
# lines 7-8: Using the split function, the values ​​of the name column are separated
# and the output is split into first_name and last_name columns
# lines 9-11: The format of the birth`s date is (DD/MM/YY), when passing the
# string to date, the program did not differentiate between the decades that belonged to 1900
# and those of 2000. To obtain the age, the year was extracted from the current date, the last two characters of the date were taken,
# step to number and the age was obtained using the formula 100+(current date - date of birth)
# lines 12-16: A csv with the postal codes and their corresponding cities was passed to df.
# the names of the cities were changed to lowercase letters to match the localities in the table
# query. The resulting df was passed to the dictionary, establishing the postl code variable as key (also defined in the
# sql query table. Finally, the values ​​of the zip codes in the query table are called, in the dictionary
# previously defined, resulting in the corresponding localities column).
# line 17: only the required columns were selected


def transform_df(df, university_id:str):
    """
    
    This function si adapted to transform the data
    of all the universities in the database
         
    Args:
        df: a p.ataFram from sql qeries
        university_id: id from the University

    output: a df and a locally saved file ('.txt)

    The function transforms the data as follows:

    Normalizes the content of string type columns 
    with special characters through the 
    'normalize_characters' function

    gender: transform strings starting with m to 'male, and the 
    rest to 'female' or 'not identified as male or female gender'

    name: having treated the special characters of this column with 
    the "normalizing data" function, they are separated into 
    'first_name' and last_ame'. If this is not possible, everything 
    is assigned to 'last_name'

    inscription_date: sets '/' for separation and '%Y/%m/%d'' as date
    format

    age: Three possible cases are taken into account for the birth_date
            formats '%d/%m/%Y' and '%Y/%m/%d' : the age is obtained by 
                                                subtracting the date of 
                                                birth from the current year
            'dd/Month/YY' format: by which the last two positions are taken
            from the string (corresponding to the ten of the year), it is 
            converted to 'int' and together with the current year 'curr', the
            age is obtained by the following formula: 100+(curr- year of birth)

    location and postal_code: depending on the column in the table, the 
    values ​​are used as input to define a dictionary, by which the values ​​of the 
    missing column will be called"""
    import pandas as pd
    import pathlib
    import datetime
    from datetime import date
    import logging
    import os
           
    path=(pathlib.Path(__file__).parent.absolute()).parent
    logging.info(f'normalizing data')

    #strngs with special characters
    df['university'] = normalize_characters(df['university'])
    df['career'] = normalize_characters(df['career'])
    df['name'] = normalize_characters(df['name'])
    df['gender'] = normalize_characters(df['gender'])
    df['nacimiento']= df['nacimiento'].apply(lambda x: str(x).replace('-', '/'))
    #gender
    try:
        df['gender'] = df['gender'].apply(
            lambda x: 'male' if x[0] == 'm' else 'female')
    except:
        logging.info('not identified as male or female gender')  

    #names
    try:
        df['first_name'] = df['name'].apply(lambda x: str(x).split(' ')[0])
        df['last_name'] = df['name'].apply(lambda x: str(x).split(' ')[1])
    except:
        df['first_name'] = 'NULL'
        df['last_name'] = df['name']
        
    #Dates
    try:
        old_date = pd.to_datetime(df['inscription_date'])
        df['inscription_date'].apply(lambda x: str(x).replace('-', '/'))
        df['inscription_date'] = pd.to_datetime(old_date, '%Y/%m/%d')
    except:
        logging.info("inscription enrollment date could not"+ 
                    "be transformed to '%Y/%m/%d' format."+
                    "current values will be kept in the column")

    #age    
    df['nacimiento']=df['nacimiento'].apply(lambda x: str(x).replace('-', '/'))
    if len(df['nacimiento'].apply(lambda x: str(x).split(' ')[0]))==4:
        df['nacimiento']= df['nacimiento'].apply(lambda x: x.strptime('Y%/%m/%d'))
        df['nacimiento']= df['nacimiento'].apply(lambda x: x.strftime('d%/%m/%Y'))

    curr= datetime.now()
    try:
        df['age'] = df['nacimiento'].apply(lambda x: (100+(int(str(curr.year)[2:4]) - int(x[7:9])) if len(x)== 9 else (curr.year - datetime.strptime(str(x), '%Y/%m/%d').year)))

    except:
        df['age'] = df['nacimiento'].apply(lambda x: curr.year - datetime.strptime(str(x), '%d/%m/%Y').year)

    #location and postal_code
    if 'postal_code' in df.columns:
        input= 'postal_code'
        output= 'location'
        key= 'codigo_postal'
        value= 'localidad'
    elif 'location' in df.columns:
        df['location'] = normalize_characters(df['location'])
        input= 'location'
        output= 'postal_code'
        key= 'localidad'
        value= 'codigo_postal'
        try:
            if os.path.exists(f'{path}/dataset/codigos_postales.csv'):
                df_postal_codes = (pd.read_csv(f'{path}/dataset/codigos_postales.csv'))
            else:
                logging.info('postal code file does not exist in the specified path')
            df_postal_codes['localidad'] = df_postal_codes['localidad'].apply(
                lambda x: x.lower())
            dict_postal_codes = dict(
            zip(df_postal_codes[key], df_postal_codes[value]))
            df[output] = df[input].apply(lambda x: dict_postal_codes[(x)])
            df['postal_code']= df['postal_code'].apply(lambda x: int(x))
        except:
            logging.info("values could not"+ 
                    "be transformed current"+
                    "values will be kept in the column")
    
    # save_data
        df = df[['university', 'career', 'inscription_date', 'first_name',
                'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
        df.to_csv(f'{path}/files/ETL_{university_id}.txt', sep='\t')
    return(df)

