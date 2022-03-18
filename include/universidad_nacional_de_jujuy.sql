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