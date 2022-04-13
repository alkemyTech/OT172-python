select universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    nombre as name,
    sexo as gender,
    fecha_nacimiento as nacimiento,
    email as email,
	 
    localidad as location,
from idinput 
where universidad = salvador_villa_maria 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc