select universidad as university,
    carrerra as career,
    fechaiscripccion as inscription_date,
    nombrre as name,
    sexo as gender,
    nacimiento as nacimiento,
    eemail as email,
	codgoposstal as postal_code,
                                   
from idinput 
where universidad = moron_nacional_pampa 
    and (date(fecha_de_inscripcion) BETWEEN startdate and  endate )
    order by date(fecha_de_inscripcion) asc