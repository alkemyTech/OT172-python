select universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    name as name,
    sexo as gender,
    fecha_nacimiento as nacimiento,
    correo_electronico as email,
	codigo_postal as postal_code,
                                   
from idinput 
where universidad = UNIVERSIDAD DE FLORES 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc