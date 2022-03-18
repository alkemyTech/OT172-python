SELECT
univiersities, carrera, inscription_dates, sexo, names, email, localidad, fechas_nacimiento 
FROM
rio_cuarto_interamericana
WHERE
univiersities = '-universidad-abierta-interamericana'
AND TO_DATE(inscription_dates,'DD/MON/YY') >= '01/09/2020'
AND TO_DATE(inscription_dates,'DD/MON/YY') <  '01/02/2021'
