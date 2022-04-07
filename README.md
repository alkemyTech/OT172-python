# base-proyect-da-python

Proceso ETL completo

La ejecución del archivo automated ET.py, ingresando las universidades en la forma en que se indica, 
da como resultado el proceso completo de genjeración de las consultas y de los dags coprrespondiente para cada caso solicitado.

Configuración:

La configuracion de cada dag se hace, designando una configuracion deseada en el archivo dags_config.json
Este archivo contendrá todas las configuraciones que el usuario desee. Para asignar estas configuraciones, se indica en la lista de la universidad  a la que corresponda. Si no se asigna ninguna
el programa tomara Config_1 como la onfiguracion por default.

Luego de configurar el archivo json, solo hay que asignar los inputs en el archivo Automated_ETL.py
y dara como resultado, archivos .sql con las consultas, y archivos.py con cada dag corrrespondiente