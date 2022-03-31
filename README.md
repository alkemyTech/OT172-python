# ETL de datos de diferentes centros educativos para el Ministerio de Educación de la Nación
## Descripción
El programa toma información de diferentes centros educativos mediante consultas sql a una base de datos postgresql, normaliza y transforma los datos, y los almacena en Amazon Simple Storage Service (Amazon S3), asi como en forma local, en archivos ".txt".

Los centros educativos contemplados, son:

- Universidad NAcional de La Pampa
- Universidad abierta Interamericana
- Universidad Tecnológica Nacional
- Universidad Tres de febrero
- Universidad de Morón
- Universidad Nacional de Rio Cuarto
- Universidad de Comahue
- Universidad del Salvador
- Universidad de Palermo
- Universidad Nacional de Jujuy
- Universidad de Flores
- Universidad NAcional Villa Maria

## Requerimientos:
- Apache-Airflow 2.2.2
- Python 3.6
## Modulos utilizados en Python
- pathlib
- logging
- pandas
- datetime
- os
- sqlalchemy

## Enlaces:
- Repositorio GitHub: https://github.com/alkemyTech/OT172-python/
- Guia de instalación de Apache Airflow en Ubuntu: https://unixcop.com/how-to-install-apache-airflow-on-ubuntu-20

## Estructura y flujo de ejecución
  Se generaron archivos ".sql" con las consultas correspondientes a cada centro educativo, normalizando las columnas tenidas en cuenta
  Mediante operadores disponibles en apache airflow (Python operators y postgre operators, se toman las consultas ".sql" para obtener los datos de la       base de datos provista. Estos datos se transorman mediante la libreria pandas, y se almacenan en forma local como archivos ".txt".
  Finalmete, a traves de las herramientas provistas por AWS (operadores y hooks S3), los datos almacenados como ".txt" son transformados a strings, y       almacenados en el servicio S3.

