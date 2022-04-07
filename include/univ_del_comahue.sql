--Datos de las pesonas anotadas entre 01/9/2020 al 01/02/2021

select
    universidad,
    carrera,
    fecha_de_inscripcion,
    name,
    sexo,
    fecha_nacimiento,
    codigo_postal,
    direccion,
    correo_electronico
from
    flores_comahue
where
    universidad = 'UNIV. NACIONAL DEL COMAHUE' 
    and (date(fecha_de_inscripcion) BETWEEN '2020-09-01' and  '2021-02-01' )
    order by date(fecha_de_inscripcion) asc