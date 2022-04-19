select univiersities as university,
    carrera as career,
    inscription_dates as inscription_date,
    names as name,
    sexo as gender,
    fechas_nacimiento as nacimiento,
    email as email,
	 
    localidad as location,
from idinput 
where univiersities = rio_cuarto_interamericana 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc