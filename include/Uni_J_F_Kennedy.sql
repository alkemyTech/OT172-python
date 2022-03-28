-- Code in syntax Postgresql

-- Obtener los datos de las pesonas anotadas en entre las fechas 
-- 01/9/2020 al 01/02/2021 de Universidad J. F. Kennedy
-- Consideraciones: psql tiene un problema NATIVO a la hora de calcular edades
--      si el formato de año de un date con solo los ultimos dos numeros de año (YY),
--      una persona de nacida en 1958, a la hora de calcular su edad sera negativa,
--      ya que 58 lo convierte a 2058, por lo que se le resta a la fecha actual.
--      Por lo tanto se debe convertir manualmente el formato YYYY-MM-DD para la fecha de nacimiento
--
--      columna nombre, muchas de las personas tienen segundo nombre y/o usan abreviaturas de 
--      titulos profesionales o formales, lo que requiere una atencion mas a detalle con pandas
SELECT
	ubaUk.universidades AS university,
	ubaUk.carreras AS career,
	ubaUk.fechas_de_inscripcion AS inscription_date,
	ubaUK.nombres as full_name,
	ubaUk.sexo AS gender,
	CASE
		WHEN SUBSTRING(ubaUk.fechas_nacimiento, 1, 2)::int > '49' THEN
               DATE_PART('year', AGE(to_date(CONCAT(
                    '19',
                    ubaUk.fechas_nacimiento
                    ), 'YYYY-Mon-DD')))
		ELSE
                DATE_PART('year', AGE(to_date(CONCAT(
                    '20',
                    ubaUk.fechas_nacimiento
                    ), 'YYYY-Mon-DD')))
	END AS age,
	ubaUk.codigos_postales AS postal_code,
	ubaUk.direcciones AS location,
	ubaUK.emails AS email
FROM
	public.uba_kenedy AS ubaUk
WHERE
	ubaUk.universidades = 'universidad-j.-f.-kennedy'
	AND to_date(fechas_de_inscripcion, 'YY-Mon-DD') 
    BETWEEN to_date('2020-Sep-01', 'YY-Mon-DD') 
        AND to_date('2021-Feb-01', 'YY-Mon-DD');
