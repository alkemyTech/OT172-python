--Query to obtain raw data on people enrolled in Universodad Interamericana
SELECT
universidad as university,
careers as career, 
fecha_de_inscripcion as inscription_date,
names as name, 
sexo as gender,
birth_dates as nacimiento, 
codigo_postal as postal_code,
correos_electronicos  as email
FROM
palermo_tres_de_febrero 
WHERE
universidad = 'universidad_nacional_de_tres_de_febrero'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') >= '01/Sep/20'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') <  '01/Feb/21';

