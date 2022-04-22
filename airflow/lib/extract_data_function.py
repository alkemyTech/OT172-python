# Data extraction function through database queries
# Start connecting to the database through the Postgres operator`s Hook
def extraction(database_id:str, table_id:str):
    """Args: 

    datbase_id: id from the pg-database 
                (previously created 
                from the airflow interface
    table_id:  id of the table to consult

    output: pandas dataframe with queries result
               a csv file with the name of the required table
               is saved locally in the "files" folder 
    

    Extraction function through queries to the database.
    Through the PostgresHook hook (https://airflow.apache.org/,\ndocs/apache-airflow-providers-postgres/stable/\n    _api/airflow/providers/postgres/hooks/postgres/index.html) 
    the connection to a posgreesql database previously created
    in the Airflow interface,taking as a parameter the ID designated 
    to the connection
    """
    logging.info('Connecting to db')
    if (not isinstance(database_id, str) or not isinstance(table_id, str)):
            logging.ERROR('input not supported. Please enter string like values')

    elif ((isinstance(database_id, str) or  isinstance(table_id, str))):
        hook = PostgresHook(postgres_conn_id=database_id)
        conn = hook.get_conn()
        logging.info(f'Conected to {table_id}')

    else:
        logging.error('ERROR: database connection failed')
    
# SQL query: To execute the query with the Hook, it must be passed as a string to the function
# pd.read_sql, along with the conn object that establishes the connection.
# The .sql file is opened and the text is saved in the query variable
    logging.info('opening sql file')
    if os.path.exists(f'{path}/include/{table_id}.sql'):
        try:        
            with open(str(f'{path}/include/{table_id}.sql')) as file:
                try:
                    query = str(text(file.read()))
                    logging.info(f'Extracting data to {file}')
                except:
                    logging.ERROR('cant read sql file')
        except:
            logging.ERROR('Open .sql file failed')
    else:
            logging.error(f'file not exist')

# The output of this function is a df with the selected rows and columns
# Finally, the df is saved as .csv
    try:
        logging.info('executing query')
        df = pd.read_sql(query, conn)
        logging.info('query successfull')
    except:
        logging.ERROR('query fail')
    df.to_csv(f'{path}/files/{table_id}.csv', sep='\t')
    logging.info('Data saved as csv')
    return None
