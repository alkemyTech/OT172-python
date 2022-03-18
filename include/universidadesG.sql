-- Code in Postgresql

-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021
-- los años estan en formato YY debemos pasarlos a YYYY por que sino tenemos problemas
-- problema con el second name, no es last name, NULL?
SELECT ubaUk.universidades AS university,
        ubaUk.carreras AS career,
        ubaUk.fechas_de_inscripcion AS inscription_date,
        SPLIT_PART(ubaUk.nombres, '-', 1) AS first_name,
        SPLIT_PART(ubaUk.nombres, '-', -1) AS second_name,
        ubaUk.sexo AS gender,
        -- convertir año de fecha nacimiento a y digitos
        CASE 
            WHEN substring(ubaUk.fechas_nacimiento, 1, 2)::int > '49' THEN
                to_date(CONCAT(
                    '19',   
                    ubaUk.fechas_nacimiento
                    ), 'YYYY-Mon-DD')
            ELSE
                to_date(CONCAT(
                    '20',
                    ubaUk.fechas_nacimiento
                    ), 'YYYY-Mon-DD')
        END AS birth_date,
        ubaUk.codigos_postales AS postal_code,
        ubaUk.direcciones AS `location`,
        ubaUK.emails AS email
    FROM public.uba_kenedy AS ubaUk 
WHERE to_date(fechas_de_inscripcion, 'YY-Mon-DD') 
    BETWEEN to_date('2020-Sep-01', 'YY-Mon-DD') 
        AND to_date('2021-Feb-01', 'YY-Mon-DD');



-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021
-- faltan los codigos postales
SELECT latCine.names 
        latCine.universities AS university,
        latCine.careers AS career,
        latCine.inscription_dates AS inscription_date,
        latCine.sexo AS gender,
        date_part('year', 
            AGE(to_date(latCine.birth_dates, 'DD-MM-YYYY'))) AS age,
        latCine.locations AS location,
        latCine.emails AS email
    FROM lat_sociales_cine AS latCine
WHERE to_date(latCine.inscription_dates, 'DD-MM-YYYY') 
    BETWEEN to_date('01-09-2020', 'DD-MM-YYYY') 
        AND to_date('01-02-2021', 'DD-MM-YYYY');
