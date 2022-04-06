from generate_dag_files import *
from generate_sql_queries import *


if __name__ == "__main__":
    main([['flores_comahue','UNIVERSIDAD DE FLORES']
            ,['jujuy_utn','universidad tecnológica nacional']
            ,['lat_sociales_cine','lat_sociales_cine']
            ,['moron_nacional_pampa','moron_nacional_pampa']
            ,['palermo_tres_de_febrero','palermo_tres_de_febrero']
            ,['rio_cuarto_interamericana','rio_cuarto_interamericana']
            ,['salvador_villa_maria','salvador_villa_maria']
            ,['uba_kenedy','uba_kenedy']])

    main_dags()