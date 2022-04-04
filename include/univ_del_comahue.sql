--Datos de las pesonas anotadas entre 01/9/2020 al 01/02/2021
select
    comahue.universidad                                 AS university,
    comahue.carrera                                     AS career,
    comahue.fecha_de_inscripcion                        AS inscription_date,
    SPLIT_PART(comahue.name,' ',1)                      AS first_name,
    REVERSE(SPLIT_PART(REVERSE(comahue.name),' ',1))    AS last_name,
    comahue.sexo                                        AS gender,
    comahue.fecha_nacimiento                            AS age,
    comahue.codigo_postal                               AS postal_code,
    comahue.direccion                                   AS location,
    comahue.correo_electronico                          AS email
from
    flores_comahue as comahue
where
    universidad = 'UNIV. NACIONAL DEL COMAHUE' 
    and (date(fecha_de_inscripcion) BETWEEN '2020-09-01' and  '2021-02-01' )
    order by date(fecha_de_inscripcion) asc