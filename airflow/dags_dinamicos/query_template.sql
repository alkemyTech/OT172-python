select universityimput as university,
    careerinput as career,
    inscription_dateinput as inscription_date,
    nameinput as name,
    genderinput as gender,
    nacimientoinput as nacimiento,
    emailinput as email,
	postalcodeinput as postal_code,
    locationinput as location,
from idinput 
where universityimput = selectuniv 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc