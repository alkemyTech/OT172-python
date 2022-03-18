SELECT universidades, carreras, fechas_de_inscripcion, nombres, sexo, fechas_de_nacimiento, codigos_postales
FROM uba_kenedy
WHERE universidades = 'universidad-de-buenos-aires' and fechas_de_inscripcion >= '01-09-2020' and fechas_de_inscripcion <= '01-02-2021';
