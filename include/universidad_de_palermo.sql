select universidad,
    careers,
    fecha_de_inscripcion,
    names,
    sexo,
    birth_dates,
    correos_electronicos,
	codigo_postal
from palermo_tres_de_febrero 
where universidad = '_universidad_de_palermo' 
    and (date(fecha_de_inscripcion) BETWEEN '01/Sep/20' and  '01/Feb/21' )
    order by date(fecha_de_inscripcion) asc