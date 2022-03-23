
-- Tarea 172-16

--Query to obtain raw data on people enrolled in Universodad Interamericana
SELECT
univiersities as university, 
carrera as career, 
inscription_dates as inscription_date, 
names as name,
sexo as gender,
fechas_nacimiento as nacimiento,
localidad as location, 
email
FROM 
rio_cuarto_interamericana
WHERE
univiersities = '-universidad-abierta-interamericana'
AND TO_DATE(inscription_dates,'DD/MON/YY') >= '01/09/2020'
AND TO_DATE(inscription_dates,'DD/MON/YY') <  '01/02/2021'
