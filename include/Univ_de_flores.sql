--consulta sql para universidad de flores

SELECT 
    universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    nombre_de_usuario as name,
    sexo as gender,
    fecha_nacimiento as nacimiento,
    codigo_postal as postal_code,
    direccion,
    correo_electronico as email
FROM
    flores_comahue
WHERE
    (fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01')    
AND
    universidad = 'UNIVERSIDAD DE FLORES'
