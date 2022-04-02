--consulta sql para la universidad de Villa Maria

SELECT 
    universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    nombre_de_usuario as name,
    sexo as gender,
    fecha_nacimiento as nacimiento,
    direccion,
    email
FROM
    salvador_villa_maria
WHERE
	to_date(fecha_de_inscripcion, 'DD-Mon-YYY') 
BETWEEN 
'2020-09-01' AND '2021-02-01' 
AND
    universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'