-- Task PT172-19
-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para
-- las siguientes facultades:
-- Universidad De Buenos Aires

select
        universidades, carreras, fechas_de_inscripcion, nombres, sexo, fechas_nacimiento, codigos_postales, direcciones, emails
from
        uba_kenedy uk
where
        TO_DATE(fechas_de_inscripcion, 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
        and universidades = 'universidad-de-buenos-aires'
;
