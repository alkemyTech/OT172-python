select universities as university,
    careers as career,
    inscription_dates as inscription_date,
    names as name,
    sexo as gender,
    birth_dates as nacimiento,
    emails as email,
	 
    locations as location,
from idinput 
where universities = lat_sociales_cine 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc