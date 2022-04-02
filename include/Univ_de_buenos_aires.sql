-- Task PT172-19
-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para
-- las siguientes facultades:
-- Universidad De Buenos Aires

select
        universidades as university, carreras as career, fechas_de_inscripcion as inscription_date,
         nombres as name, sexo as gender, fechas_nacimiento as nacimiento, codigos_postales as postal_code, direcciones, emails
from
        uba_kenedy uk
where
        TO_DATE(fechas_de_inscripcion, 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
        and universidades = 'universidad-de-buenos-aires'
;
