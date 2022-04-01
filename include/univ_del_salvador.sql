--Datos de las pesonas anotadas entre 01/9/2020 al 01/02/2021
--La parte de codigo postal se tiene que procesar el archivo extra para sacarlo a partir de la localidad

select
    salvador.universidad                                AS university,
    salvador.carrera                                    AS career,
    salvador.fecha_de_inscripcion                       AS inscription_date,
    SPLIT_PART(salvador.name,' ',1)                     AS first_name,
    REVERSE(SPLIT_PART(REVERSE(salvador.name),' ',1))   AS last_name,
    salvador.sexo                                       AS gender,
    salvador.fecha_nacimiento                           AS age,
    salvador.direccion                                  AS location,
    salvador.email                                      AS email
from
    salvador_villa_maria as salvador
where
    universidad = 'UNIVERSIDAD_DEL_SALVADOR' 
    and (date(fecha_de_inscripcion) BETWEEN '01-Sep-2020' and  '01-Feb-2021' )
    order by date(fecha_de_inscripcion) asc