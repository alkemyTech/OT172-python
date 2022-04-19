select university as university,
    career as career,
    inscription_date as inscription_date,
    nombre as name,
    sexo as gender,
    birth_date as nacimiento,
    email as email,
	 
    location as location,
from idinput 
where university = universidad tecnológica nacional 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc