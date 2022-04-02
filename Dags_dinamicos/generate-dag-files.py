import json
import os
import shutil
import fileinput
import pathlib

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

                keys=[key for key in dict_config]

                if (dict_config==None) or (id not in (keys)):
                    Config= 'Config_1'
                else:
                    Config= dict_config[str(id)]
                print(id+' '+ Config)
                for line in fileinput.input(new_filename, inplace=True):
                    line= line.replace("dagid_toreplace", "'"+id+"'")
                    line= line.replace("scheduler_toreplace","'"+config[Config][0]['Schedule']+"'")
                    print(line, end="")


def main():
    create_dags(dict_config= {"Univ_de_buenos_aires": "Config_2",
    "Univ_nacional_de_jujuy": "Config_3",
    "Univ_nacional_LaPampa": "Config_2"})

if __name__ == "__main__":
    main()
