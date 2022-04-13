select universidad as university,
    careers as career,
    fecha_de_inscripcion as inscription_date,
    names as name,
    sexo as gender,
    birth_dates as nacimiento,
    correos_electronicos as email,
	codigo_postal as postal_code,
                                   
from idinput 
where universidad = palermo_tres_de_febrero 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc