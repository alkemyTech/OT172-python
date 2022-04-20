select universidades as university,
    carreras as career,
    fechas_de_inscripcion as inscription_date,
    nombres as name,
    sexo as gender,
    fechas_nacimiento as nacimiento,
    emails as email,
	codigos_postales as postal_code,
                                   
from idinput 
where universidades = uba_kenedy 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc