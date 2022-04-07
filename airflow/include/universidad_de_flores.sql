--- """consulta sql para universidad de flores"""

SELECT 
    universidad,
    carrera,
    fecha_de_inscripcion,
    nombre_de_usuario,
    sexo,
    fecha_nacimiento,
    codigo_postal,
    direccion,
    correo_electronico
FROM
    flores_comahue
WHERE
    (fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01')    
AND
    universidad = 'UNIVERSIDAD DE FLORES'
