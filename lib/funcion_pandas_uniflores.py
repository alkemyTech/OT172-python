"""
Pandas Uni A Leonardo

Una funcion que devuelva un txt para cada una de las siguientes universidades con los datos normalizados:
Universidad De Flores
Universidad Nacional De Villa María

Datos Finales:
university: str minúsculas, sin espacios extras, ni guiones
career: str minúsculas, sin espacios extras, ni guiones
inscription_date: str %Y-%m-%d format
first_name: str minúscula y sin espacios, ni guiones
last_name: str minúscula y sin espacios, ni guiones
gender: str choice(male, female)
age: int
postal_code: str
location: str minúscula sin espacios extras, ni guiones
email: str minúsculas, sin espacios extras, ni guiones

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos

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

"""
