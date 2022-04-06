import json
import os
import shutil
import fileinput
import pathlib
from datetime import date
from pendulum import datetime

def create_dags(dict_config=None):
        path = (pathlib.Path(__file__).parent.absolute()).parent
        config_filepath = f'{path}/Dags_dinamicos/'
        sql_files= f'{path}/include'
        dag_template_filename = f'{path}/Dags_dinamicos/dag-template.py'

        for filename in os.listdir(config_filepath):
            f = open(config_filepath + 'dags_config.json')
            config = json.load(f)
   
            for file in os.listdir(sql_files):
                id= file[:-4]
            
                new_filename = f'{path}/dags/'+id+'_dags.py'
                shutil.copyfile(dag_template_filename, new_filename)


                if (dict_config==None) or (id not in ([key for key in dict_config])):
                    Config= 'Config_1'
                else:
                    Config= dict_config[str(id)]
                print(id+' '+ Config)
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
                    print(line, end="")


def main():
    create_dags()

if __name__ == "__main__":
    main()
