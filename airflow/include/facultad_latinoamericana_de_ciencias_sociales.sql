-- Code in syntax Postgresql

-- Obtener los datos de las pesonas anotadas en entre las fechas 
-- 01/9/2020 al 01/02/2021 de Facultad Latinoamericana De Ciencias Sociales
-- Consideraciones: se concluye que en la columna "names" el primero de ellos 
--      es firts name y el ultimo es last name
-- 
--      Falta la columna "postal_code"
SELECT 
    latCine.universities AS university,
    latCine.careers AS career,
    latCine.inscription_dates AS inscription_date,
    SPLIT_PART(latCine.names, '-', 1) AS first_name,
    REVERSE(SPLIT_PART(REVERSE(latCine.names), '-', 1)) AS last_name,
    latCine.sexo AS gender,
    date_part('year', 
        AGE(to_date(latCine.birth_dates, 'DD-MM-YYYY'))) AS age,
    latCine.locations AS location,
    latCine.emails AS email
FROM 
    lat_sociales_cine AS latCine
WHERE 
    latCine.universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' 
    AND to_date(latCine.inscription_dates, 'DD-MM-YYYY') 
    BETWEEN to_date('01-09-2020', 'DD-MM-YYYY') 
        AND to_date('01-02-2021', 'DD-MM-YYYY');