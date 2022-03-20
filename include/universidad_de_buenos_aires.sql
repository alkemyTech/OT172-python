select
        universidades, carreras, fechas_de_inscripcion, nombres, sexo, fechas_nacimiento, codigos_postales
from
        uba_kenedy uk
where
        TO_DATE(fechas_de_inscripcion, 'YY-MON-DD') between '2020-09-01' and '2021-02-01'
        and universidades = 'universidad-de-buenos-aires'
;
