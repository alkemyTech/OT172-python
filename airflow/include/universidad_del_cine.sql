-- Task PT172-19
-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para
-- las siguientes facultades:
-- Universidad Del Cine

select
        universities, careers, inscription_dates, names, sexo, birth_dates, locations, direccion, emails
from
        lat_sociales_cine
where
        TO_DATE(inscription_dates, 'DD-MM-YYYY') between '2020-09-01' and '2021-02-01'
        and universities = 'UNIVERSIDAD-DEL-CINE'
;
