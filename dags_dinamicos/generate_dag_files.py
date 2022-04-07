import json
import os
import shutil
import fileinput
import pathlib
from datetime import date
from pendulum import datetime

def create_dags(dict_config=None):
    """"Create dags" creates the necessary .py files to generate dags 
       in Apache Airflow, with the configuration chosen in each case.
       Args:
       
       dict_config:  A dictionary with the University as key, and 
       -configuration chosen as value. 
       The configuration is located in the "dags_config.json" file.
       if no configuration is assigned (the university does not appear
       in the dictionary), the program will default "Config_1"
       
       Initially, the location of the configuration file for the dag
       will have, the path where the .sql queries should be saved, 
       and the full path where the template that will be used to 
       generate the dags 'dag-template-py is located are configured
       Then the .json file with the configuration set is opened

       Finally, the function iterates over the files with the sql 
       queries:
       1- asign the filename as "id"
       2- create a .py file from the template "dagtemplate.py"
       3- configue the file name with the id of the corresponding 
       university
       4- It assigns the configuration that has been indicated 
       (Config_1 if none was indicated).
       5- Iterate over the lines of the file and replace the values 
       corresponding to each dag, customizing the file
       """
    path = (pathlib.Path(__file__).parent.absolute()).parent
    config_filepath = f'{path}/dags_dinamicos/'
    sql_files= f'{path}/include'
    dag_template_filename = f'{path}/dags_dinamicos/dag-template.py'

    for filename in os.listdir(config_filepath):
        f = open(config_filepath + 'dags_config.json')
        config = json.load(f)
        list_Dags= []
        for file in os.listdir(sql_files):
            id= file[:-4]
            list_Dags.append(id)
            
            new_filename = f'{path}/dags/'+id+'_dags.py'
            shutil.copyfile(dag_template_filename, new_filename)


            if (dict_config==None) or (id not in ([key for key in dict_config])):
                Config= 'Config_1'
            else:
                Config= dict_config[str(id)]
            for line in fileinput.input(new_filename, inplace=True):
                line= line.replace("dagid_toreplace", "'"+id+"'")
                line= line.replace("owner_toreplace","'"+config[Config][0]['owner']+"'")
                line= line.replace("depends_on_past_toreplace",config[Config][0]['depends_on_past'])
                line= line.replace("email_on_failure_toreplace",config[Config][0]['email_on_failure'])
                line= line.replace("email_on_retry_toreplace",config[Config][0]['email_on_retry'])
                line= line.replace("retries_toreplace",config[Config][0]['retries'])
                line= line.replace("retry_delay_toreplace", config[Config][0]['retry_delay'])
    
                line= line.replace("start_date_toreplace",config[Config][0]['start_date'])
                line= line.replace("max_active_runs_toreplace",config[Config][0]['max_active_runs'])
                line= line.replace("schedule_interval_toreplace","'"+config[Config][0]['schedule_interval']+"'")
                line= line.replace("template_searchpath_toreplace",config[Config][0]['template_searchpath']+"'")
                line= line.replace("catchup_toreplace",config[Config][0]['catchup'])
                line= line.replace("fechadia",str(date.today()))
                line= line.replace("rutasql", sql_files)
                print(line, end="")


def main_dags():
    create_dags()

if __name__ == "__main__":
    main_dags()
    help(create_dags)