-- Codigo para base de datos postgresql
-- sql para universidad de palermo
-- Obtener los datos de las pesonas anotadas entre las fechas
-- 01/9/2020 al 01/02/2021
-- Al campo fecha_de_inscripcion se le pasa el metodo date, para poder trabajar,
-- ya que esta en un formato dd/mes/yy
-- se ordenan por fecha de incripcion ascendente

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