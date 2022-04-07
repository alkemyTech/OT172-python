--Datos de las pesonas anotadas entre 01/9/2020 al 01/02/2021
--La parte de codigo postal se tiene que procesar el archivo extra para sacarlo a partir de la localidad

select
    universidad,
    carrera,
    fecha_de_inscripcion,
    nombre,
    sexo,
    fecha_nacimiento,
    codigo_postal,
    direccion,
    email
from
    salvador_villa_maria
where
    universidad = 'UNIVERSIDAD_DEL_SALVADOR' 
    and (date(fecha_de_inscripcion) BETWEEN '01-Sep-2020' and  '01-Feb-2021' )
    order by date(fecha_de_inscripcion) asc