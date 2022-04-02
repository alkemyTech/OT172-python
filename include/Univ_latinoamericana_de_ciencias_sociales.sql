-- Code in syntax Postgresql

-- Obtener los datos de las pesonas anotadas en entre las fechas 
-- 01/9/2020 al 01/02/2021 de Facultad Latinoamericana De Ciencias Sociales
--
-- columna nombre: muchas de las personas tienen segundo nombre y/o usan abreviaturas de 
-- titulos profesionales o formales, lo que requiere una atencion mas a detalle con pandas
-- 
--  Falta la columna "postal_code"
SELECT 
    latCine.universities AS university,
    latCine.careers AS career,
    latCine.inscription_dates AS inscription_date,
    latCine.names AS name,
    latCine.sexo AS gender,
    latCine.birth_dates as nacimiento,
    latCine.locations AS location,
    latCine.emails AS email
FROM 
    lat_sociales_cine AS latCine
WHERE 
    latCine.universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' 
    AND to_date(latCine.inscription_dates, 'DD-MM-YYYY') 
    BETWEEN to_date('01-09-2020', 'DD-MM-YYYY') 
        AND to_date('01-02-2021', 'DD-MM-YYYY');
