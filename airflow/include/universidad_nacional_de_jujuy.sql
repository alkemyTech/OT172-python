-- Codigo para base de datos postgresql
-- sql para universidad nacional de jujuy
-- Obtener los datos de las pesonas anotadas entre las fechas
-- 01/9/2020 al 01/02/2021

-- se ordenan por fecha de incripcion ascendente

select university,
    career,
    inscription_date,
    nombre,
    sexo,
    birth_date,
    location,
    email
from jujuy_utn 
where university = 'universidad nacional de jujuy' 
    and (inscription_date BETWEEN '2020-09-01' and  '2021-02-01' )
    order by inscription_date asc